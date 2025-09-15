import os
import base64
import time
import json
import logging
from typing import Any, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

# --- Library to load .env file for local development ---
from dotenv import load_dotenv

import requests
from flask import Flask, jsonify
from google.cloud import storage

# --- Load environment variables from .env file for local development ---
load_dotenv()

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================================================================
# 1. Configuration and Constants
# ==============================================================================
app = Flask(__name__)
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

# --- Performance and Stability Tuning ---
MAX_WORKERS = 30
# *** FIX: Lowered the concurrency limit to a safer value to avoid 429 errors. ***
API_CONCURRENCY_LIMIT = 10
MAX_RETRIES = 3 # Number of times to retry a failed API request

# Batches for enrichment endpoints
ALBUM_BATCH_SIZE = 20
TRACK_BATCH_SIZE = 50

# --- Rate Limiting Semaphore ---
api_semaphore = Semaphore(API_CONCURRENCY_LIMIT)

# --- Best Practice: Use a Session object for connection pooling ---
http_session = requests.Session()

# Initialize GCS client
try:
    storage_client = storage.Client()
    gcs_bucket = storage_client.bucket(GCS_BUCKET_NAME) if GCS_BUCKET_NAME else None
except Exception as e:
    storage_client, gcs_bucket = None, None
    logging.error(f"Fatal: Could not initialize GCS client. Error: {e}", exc_info=True)

# ==============================================================================
# 2. Access Token Caching
# ==============================================================================
_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0}

def get_access_token() -> str:
    """Retrieves a Spotify API access token, caching it."""
    if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
        return _token_cache["token"]
    
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")).decode("utf-8")
    response = requests.post(
        SPOTIFY_AUTH_URL,
        headers={"Authorization": f"Basic {auth_header}"},
        data={"grant_type": "client_credentials"}
    )
    response.raise_for_status()
    token_info = response.json()
    _token_cache["token"] = token_info["access_token"]
    _token_cache["expires_at"] = time.time() + token_info["expires_in"] - 60
    return _token_cache["token"]

# ==============================================================================
# 3. Core Helper Functions
# ==============================================================================

def upload_to_gcs(bucket: storage.Bucket, destination_blob_name: str, data: Dict[str, Any]):
    """Uploads a dictionary to a GCS bucket as a JSON file."""
    if not bucket: raise ConnectionError("GCS bucket is not configured.")
    try:
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(data).encode("utf-8"), content_type="application/json")
    except Exception as e:
        logging.error(f"GCS upload failed for gs://{bucket.name}/{destination_blob_name}: {e}")
        raise

def fetch_spotify_data_throttled(session: requests.Session, url: str, headers: Dict[str, str], params: Dict = None) -> Dict:
    """
    A thread-safe, rate-limit-aware, and resilient function to fetch data from Spotify.
    - Uses a semaphore to limit active concurrent requests.
    - Uses a session object for efficient connection pooling.
    - Automatically retries on 429 (Too Many Requests) errors with exponential backoff.
    """
    with api_semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                response = session.get(url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                # *** FIX: Robust retry logic for 429 errors. ***
                if e.response.status_code == 429:
                    # Use the 'Retry-After' header from Spotify, or default to exponential backoff
                    retry_after = int(e.response.headers.get("Retry-After", (2 ** attempt)))
                    logging.warning(
                        f"Rate limited on attempt {attempt + 1}/{MAX_RETRIES}. "
                        f"Waiting {retry_after} seconds before retrying {url}."
                    )
                    time.sleep(retry_after)
                else:
                    # For other HTTP errors (404, 500, etc.), fail immediately.
                    logging.error(f"Non-retriable HTTP Error fetching {url}: {e}")
                    raise
            except (requests.exceptions.RequestException) as e:
                logging.error(f"A network error occurred: {e}")
                # For network errors, a simple backoff is often sufficient.
                time.sleep(2 ** attempt)
        
        # If all retries fail, raise the last error.
        raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} attempts.")

# ==============================================================================
# 4. Main ETL Orchestration
# ==============================================================================

def run_full_etl_process(artist_name: str) -> Dict[str, Any]:
    """Orchestrates the entire ETL pipeline from artist name to GCS upload."""
    start_time = time.time()
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # STEP 1 & 2: Get Artist ID and Details
    search_url = f"{SPOTIFY_API_BASE_URL}/search"
    search_params = {"q": artist_name, "type": "artist", "limit": 1}
    search_results = fetch_spotify_data_throttled(http_session, search_url, headers, search_params)
    artist_items = search_results.get("artists", {}).get("items", [])
    if not artist_items: raise ValueError(f"Artist '{artist_name}' not found.")
    artist_id = artist_items[0]['id']

    artist_url = f"{SPOTIFY_API_BASE_URL}/artists/{artist_id}"
    artist_details = fetch_spotify_data_throttled(http_session, artist_url, headers)
    logging.info(f"Found Artist: {artist_details['name']} ({artist_id})")

    # STEP 3: Gather Simplified Albums
    logging.info("Gathering all album IDs...")
    simplified_albums = []
    album_url = f"{SPOTIFY_API_BASE_URL}/artists/{artist_id}/albums?limit=50"
    first_album_page = fetch_spotify_data_throttled(http_session, album_url, headers)
    simplified_albums.extend(first_album_page.get("items", []))
    total_albums = first_album_page.get("total", 0)
    album_page_urls = [f"{album_url}&offset={offset}" for offset in range(50, total_albums, 50)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_spotify_data_throttled, http_session, url, headers) for url in album_page_urls]
        for future in as_completed(futures):
            simplified_albums.extend(future.result().get("items", []))
    album_ids = list(set([album['id'] for album in simplified_albums])) # Use set to ensure uniqueness
    logging.info(f"Gathered {len(album_ids)} unique album IDs.")

    # STEP 4: Enrich Albums
    logging.info("Enriching albums with popularity data...")
    enriched_albums = []
    album_id_chunks = [album_ids[i:i + ALBUM_BATCH_SIZE] for i in range(0, len(album_ids), ALBUM_BATCH_SIZE)]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        enrich_url = f"{SPOTIFY_API_BASE_URL}/albums"
        futures = [executor.submit(fetch_spotify_data_throttled, http_session, enrich_url, headers, {"ids": ",".join(chunk)}) for chunk in album_id_chunks]
        for future in as_completed(futures):
            enriched_albums.extend(future.result().get('albums', []))
    logging.info(f"Enriched {len(enriched_albums)} albums.")

    # STEP 5 & 6: Gather and Enrich Tracks
    logging.info("Gathering and enriching all tracks...")
    all_track_ids = []
    enriched_tracks = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks to get simplified tracks from each album
        track_page_futures = {executor.submit(fetch_spotify_data_throttled, http_session, f"{SPOTIFY_API_BASE_URL}/albums/{album_id}/tracks?limit=50", headers) for album_id in album_ids}
        for future in as_completed(track_page_futures):
            track_page = future.result()
            all_track_ids.extend([track['id'] for track in track_page.get("items", []) if track])
    
    logging.info(f"Gathered {len(all_track_ids)} total track IDs.")
    
    # Enrich the gathered track IDs in batches
    track_id_chunks = [all_track_ids[i:i + TRACK_BATCH_SIZE] for i in range(0, len(all_track_ids), TRACK_BATCH_SIZE)]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        enrich_url = f"{SPOTIFY_API_BASE_URL}/tracks"
        track_enrich_futures = [executor.submit(fetch_spotify_data_throttled, http_session, enrich_url, headers, {"ids": ",".join(chunk)}) for chunk in track_id_chunks]
        for future in as_completed(track_enrich_futures):
            enriched_tracks.extend(future.result().get('tracks', []))
    logging.info(f"Enriched {len(enriched_tracks)} tracks.")
    
    # STEP 7: Upload All Data
    logging.info("Uploading all data to Google Cloud Storage...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        upload_futures = [executor.submit(upload_to_gcs, gcs_bucket, f"artists/{artist_id}.json", artist_details)]
        for item in enriched_albums + enriched_tracks:
            if item:
                folder = "albums" if item['type'] == 'album' else "tracks"
                upload_futures.append(executor.submit(upload_to_gcs, gcs_bucket, f"{folder}/{item['id']}.json", item))
        
        for future in as_completed(upload_futures):
            future.result()

    total_time = time.time() - start_time
    logging.info(f"SUCCESS: Full process for '{artist_name}' completed in {total_time:.2f} seconds.")

    return {
        "artist_name": artist_details["name"], "artist_id": artist_id,
        "albums_stored": len(enriched_albums), "tracks_stored": len(enriched_tracks),
        "processing_time_seconds": round(total_time, 2)
    }

# ==============================================================================
# 5. Flask API Route
# ==============================================================================
@app.route("/")
def index():
    return "Spotify Full ETL (Enrichment Version). Usage: /artist/&lt;artist_name&gt;/store", 200

@app.route("/artist/<string:artist_name>/store", methods=['GET'])
def store_artist_data_endpoint(artist_name: str):
    """API endpoint to trigger the full, multi-stage ETL process."""
    if not gcs_bucket:
        return jsonify({"error": "GCS bucket is not configured correctly."}), 500
    try:
        summary = run_full_etl_process(artist_name)
        return jsonify({"message": "Successfully stored artist, albums, and tracks in GCS.", **summary})
    except Exception as e:
        logging.error(f"A critical error occurred in the main endpoint: {e}", exc_info=True)
        return jsonify({"error": "An unexpected server error occurred", "details": str(e)}), 500

# ==============================================================================
# 6. Main Execution Block
# ==============================================================================
if __name__ == "__main__":
    if not all([CLIENT_ID, CLIENT_SECRET, GCS_BUCKET_NAME]):
        raise SystemExit("ERROR: Ensure SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, and GCS_BUCKET_NAME are set.")
    if not gcs_bucket:
         raise SystemExit(f"ERROR: Could not connect to GCS bucket '{GCS_BUCKET_NAME}'.")
    app.run(debug=True, host="0.0.0.0", port=8080)