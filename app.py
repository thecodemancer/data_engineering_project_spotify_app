import os
import base64
import time
import json
import logging
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, jsonify
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError

# ==============================================================================
# 1. Configuration and Constants
# ==============================================================================

# --- Basic Logging Setup ---
# Configure logging to show timestamps, log level, and messages.
# This helps in monitoring the application's execution.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Load Configuration from Environment Variables ---
# Best practice for security: sensitive info like credentials should not be hardcoded.
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# --- Centralize Spotify API URLs and Settings ---
# Makes the code cleaner and easier to update if API endpoints change.
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"
API_REQUEST_LIMIT = 50  # The max number of items Spotify returns per page.

# --- Performance Tuning ---
# Defines the number of parallel threads for fetching and uploading data.
# This provides a significant speedup for I/O-bound tasks.
MAX_WORKERS = 20

# --- Initialize Google Cloud Storage Client ---
# The client is initialized globally so it can be reused across requests,
# which is more efficient than creating a new client each time.
try:
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(GCS_BUCKET_NAME) if GCS_BUCKET_NAME else None
except Exception as e:
    storage_client, gcs_bucket = None, None
    logging.error(f"Fatal: Could not initialize GCS client. Check credentials and configuration. Error: {e}")

# ==============================================================================
# 2. Access Token Caching
# ==============================================================================

# In-memory cache for the Spotify access token.
# Spotify tokens are valid for 1 hour, so re-fetching on every request is inefficient.
# This simple cache stores the token and its expiration time.
_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0}

def get_access_token() -> str:
    """Fetches a Spotify API access token, using a cache to avoid redundant requests."""
    # If a valid, non-expired token exists, return it immediately.
    if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
        return _token_cache["token"]
    
    # If not, fetch a new one using the Client Credentials Flow.
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")).decode("utf-8")
    response = requests.post(
        SPOTIFY_AUTH_URL,
        headers={"Authorization": f"Basic {auth_header}"},
        data={"grant_type": "client_credentials"}
    )
    response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx).
    token_info = response.json()
    
    # Store the new token and calculate its expiration time (with a 60s buffer for safety).
    _token_cache["token"] = token_info["access_token"]
    _token_cache["expires_at"] = time.time() + token_info["expires_in"] - 60
    
    return _token_cache["token"]

# ==============================================================================
# 3. Helper Functions (Spotify API and GCS Interaction)
# ==============================================================================

def upload_to_gcs(bucket: storage.Bucket, destination_blob_name: str, data: Dict[str, Any]):
    """Uploads a dictionary as a compact, single-line JSON file to GCS."""
    blob = bucket.blob(destination_blob_name)
    
    # **THE CRITICAL FIX**:
    # Removed 'indent=4' to create a compact, single-line JSON string. This is required
    # by BigQuery's NEWLINE_DELIMITED_JSON format, which expects one full object per line.
    # We also explicitly encode to "utf-8" to prevent any encoding issues (like a BOM).
    blob.upload_from_string(
        json.dumps(data).encode("utf-8"),
        content_type="application/json"
    )

def get_artist_data(artist_name: str, access_token: str) -> Dict[str, Any]:
    """Searches for an artist using the Spotify Search API and returns their data."""
    search_url = f"{SPOTIFY_API_BASE_URL}/search"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": artist_name, "type": "artist", "limit": 1}
    response = requests.get(search_url, headers=headers, params=params)
    response.raise_for_status()
    search_results = response.json()
    artists = search_results.get("artists", {}).get("items", [])
    if not artists:
        raise ValueError(f"Artist '{artist_name}' not found.")
    return artists[0]

def fetch_and_store_album_tracks(album: Dict[str, Any], access_token: str) -> int:
    """
    WORKER FUNCTION: Fetches all tracks for a single album and uploads them to GCS.
    This function is designed to be run in a separate thread for concurrency.
    Returns the number of tracks processed for this album.
    """
    album_id = album["id"]
    track_count = 0
    headers = {"Authorization": f"Bearer {access_token}"}
    tracks_next_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}/tracks?limit={API_REQUEST_LIMIT}"
    
    # Paginate through all tracks for this specific album.
    while tracks_next_url:
        try:
            track_response = requests.get(tracks_next_url, headers=headers)
            track_response.raise_for_status()
            track_data = track_response.json()

            for track in track_data.get("items", []):
                # Inject the album's ID into the track data for relational mapping in BigQuery.
                track["album_id"] = album_id
                upload_to_gcs(gcs_bucket, f"tracks/{track['id']}.json", track)
                track_count += 1
            
            # Get the URL for the next page of tracks, or None if it's the last page.
            tracks_next_url = track_data.get("next")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch tracks for album {album_id}. Stopping for this album. Error: {e}")
            break # Stop processing this album on error to avoid partial data.
    return track_count

def process_and_store_artist_discography(artist_id: str, access_token: str) -> Dict[str, Any]:
    """
    OPTIMIZED: Fetches all albums first, then concurrently fetches tracks for all albums.
    """
    if not gcs_bucket:
        raise ConnectionError("GCS bucket is not configured or initialized.")

    start_time = time.time()
    all_albums: List[Dict[str, Any]] = []
    
    # STEP 1: Sequentially fetch all album metadata. This is fast as it doesn't involve deep nesting.
    logging.info(f"Starting to fetch all albums for artist {artist_id}...")
    next_url = f"{SPOTIFY_API_BASE_URL}/artists/{artist_id}/albums?limit={API_REQUEST_LIMIT}&include_groups=album,single"
    while next_url:
        response = requests.get(next_url, headers={"Authorization": f"Bearer {access_token}"})
        response.raise_for_status()
        album_data = response.json()
        all_albums.extend(album_data.get("items", []))
        next_url = album_data.get("next")
    
    logging.info(f"Found {len(all_albums)} total albums. Storing album JSON files...")
    # Store the album JSON files. This is also fast.
    for album in all_albums:
        upload_to_gcs(gcs_bucket, f"albums/{album['id']}.json", album)

    # STEP 2: Concurrently process the tracks for every album. This is the slowest part and
    # benefits the most from parallelization.
    logging.info(f"Fetching tracks for all {len(all_albums)} albums using up to {MAX_WORKERS} workers...")
    total_tracks_processed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create a "future" for each task. A future is a placeholder for a result
        # that will be available later.
        future_to_album = {executor.submit(fetch_and_store_album_tracks, album, access_token): album for album in all_albums}
        
        # as_completed() yields futures as they finish, allowing us to process results immediately
        # without waiting for all tasks to be done.
        for future in as_completed(future_to_album):
            album_name = future_to_album[future]["name"]
            try:
                # Get the result from the completed future (the number of tracks processed).
                tracks_processed = future.result()
                total_tracks_processed += tracks_processed
            except Exception as e:
                logging.error(f"An error occurred in a worker thread for album '{album_name}': {e}", exc_info=True)

    end_time = time.time()
    logging.info(f"Completed all concurrent tasks in {end_time - start_time:.2f} seconds.")
    
    return {
        "albums": len(all_albums),
        "tracks": total_tracks_processed,
        "duration_seconds": round(end_time - start_time, 2)
    }

# ==============================================================================
# 4. Flask API Routes
# ==============================================================================

@app.route("/")
def index():
    """Home endpoint with basic usage instructions."""
    return "Spotify to GCS ETL service. Usage: /artist/&lt;artist_name&gt;/store", 200

@app.route("/artist/<string:artist_name>/store", methods=['GET'])
def store_artist_data_endpoint(artist_name: str):
    """Main API endpoint to trigger the ETL process for a given artist."""
    try:
        # Get a valid access token.
        access_token = get_access_token()
        
        # Fetch and store the main artist data first.
        artist_data = get_artist_data(artist_name, access_token)
        upload_to_gcs(gcs_bucket, f"artists/{artist_data['id']}.json", artist_data)
        
        # Trigger the main processing function for albums and tracks.
        summary = process_and_store_artist_discography(artist_data['id'], access_token)
        
        # Return a success summary.
        return jsonify({
            "message": "Successfully processed and stored artist data in GCS.",
            "artist_name": artist_data["name"],
            "artist_id": artist_data['id'],
            "albums_stored": summary["albums"],
            "tracks_stored": summary["tracks"],
            "processing_time_seconds": summary.get("duration_seconds")
        })
    
    # Catch specific, expected errors and return appropriate HTTP status codes.
    except ValueError as e:
        return jsonify({"error": "Artist not found", "details": str(e)}), 404
    except (ConnectionError, GoogleAPICallError) as e:
        return jsonify({"error": "Failed to communicate with Google Cloud Storage", "details": str(e)}), 500
    except requests.exceptions.HTTPError as e:
        error_details = {"message": str(e), "url": e.request.url}
        if e.response:
            try: error_details.update(e.response.json())
            except ValueError: pass
        return jsonify({"error": "Error communicating with Spotify API", "details": error_details}), e.response.status_code if e.response else 500
    # Catch any other unexpected errors for general server-side failure.
    except Exception as e:
        logging.error(f"An unexpected server error occurred in the main endpoint: {e}", exc_info=True)
        return jsonify({"error": "An unexpected server error occurred", "details": str(e)}), 500

# ==============================================================================
# 5. Main Execution Block
# ==============================================================================

# This block runs when the script is executed directly (e.g., `python app.py`).
if __name__ == "__main__":
    # "Fail fast": Check for essential configuration at startup.
    # If any required environment variables are missing, the app will exit immediately
    # with a clear error message instead of failing later during a request.
    if not all([CLIENT_ID, CLIENT_SECRET, GCS_BUCKET_NAME]):
        raise SystemExit("ERROR: SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, and GCS_BUCKET_NAME environment variables must be set.")
    if not gcs_bucket:
         raise SystemExit(f"ERROR: Could not connect to GCS bucket '{GCS_BUCKET_NAME}'. Check configuration and permissions.")
    
    # Start the Flask development server.
    # `host="0.0.0.0"` makes the server accessible from other machines on the network.
    # In production, this script would be run by a proper WSGI server like Gunicorn.
    app.run(debug=True, host="0.0.0.0", port=8080)