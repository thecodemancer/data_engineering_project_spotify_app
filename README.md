# Data Engineering Project: Spotify ETL App

This project is a complete ETL (Extract, Transform, Load) pipeline that extracts artist data from the Spotify API, stages it in Google Cloud Storage, and provides the commands to load it into Google BigQuery. The entire infrastructure is managed with Terraform, and the application is designed to be deployed as a serverless container on Google Cloud Run.

## Features

-   **HTTP-Triggered ETL**: The pipeline is initiated via a simple API call to a web application built with the Flask framework.
-   **Optimized & Concurrent**: Fetches album and track data concurrently using multithreading for significant performance gains.
-   **Infrastructure as Code (IaC)**: All Google Cloud resources (GCS Bucket, BigQuery Dataset & Tables, Artifact Registry) are defined and managed using Terraform.
-   **Containerized**: The Python application is containerized using Docker, ensuring a consistent and portable environment.
-   **Serverless Deployment**: Deploys to Google Cloud Run, providing a scalable, cost-efficient, and fully managed platform.

## Architecture Flow

1.  **User Trigger**: An HTTP GET request is sent to the Cloud Run endpoint (e.g., `/artist/Queen/store`).
2.  **Cloud Run**: Receives the request and spins up a container instance to run the application. Inside the container, a production-grade server (Gunicorn) serves the Flask application.
3.  **Flask Application**: The Python code, using the Flask framework, routes the request and authenticates with the Spotify API.
4.  **Extract & Transform**: The app fetches all data for the specified artist, including albums and tracks. It performs a minor transformation by injecting the `album_id` into each track object.
5.  **Load to Staging**: The application uploads the data as individual, newline-delimited JSON files into a Google Cloud Storage (GCS) bucket, organized into `artists/`, `albums/`, and `tracks/` folders.
6.  **Load to Data Warehouse**: The user runs `bq load` commands to load the staged JSON files from GCS into the final, structured BigQuery tables.

## Prerequisites

-   A Google Cloud Project with billing enabled.
-   A Spotify Developer Account to get API credentials.
-   [Google Cloud SDK (`gcloud`)](https://cloud.google.com/sdk/docs/install) installed and authenticated (`gcloud auth login`, `gcloud auth application-default login`).
-   [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli) installed.
-   [Docker](https://docs.docker.com/get-docker/) installed and running.
-   Python 3.9+ and `pip`.

## Setup and Configuration

### 1. Clone the Repository
```bash
git clone [YOUR_REPOSITORY_URL]
cd data_engineering_project_spotify_app
```

### 2. Configure Spotify Credentials (Local)
Create a file named `.env` for local development. **This file should not be committed to git.**

```bash
# .env
SPOTIFY_CLIENT_ID="Your_Spotify_Client_ID_Here"
SPOTIFY_CLIENT_SECRET="Your_Spotify_Client_Secret_Here"
```

### 3. Configure Google Cloud Resources (Terraform)
Navigate to the `terraform` directory. All infrastructure configuration is managed in the `terraform.tfvars` file. Create this file and add the following content, replacing the placeholder values.

```terraform
# terraform/terraform.tfvars

project_id                  = "your-gcp-project-id"
region                      = "us-central1"
gcs_bucket_name             = "your-globally-unique-bucket-name"
bigquery_dataset_name       = "spotify_data"
staging_file_ttl_days       = 7
artifact_registry_repo_name = "spotify-etl-repo"
```

## Infrastructure Deployment with Terraform

These commands will create the GCS bucket, the BigQuery dataset with its tables, and the Artifact Registry repository based on your `terraform.tfvars` file.

1.  **Navigate to the terraform directory:**
    ```bash
    cd terraform
    ```

2.  **Initialize Terraform:**
    ```bash
    terraform init
    ```

3.  **Preview the changes:**
    ```bash
    terraform plan
    ```

4.  **Apply the changes to create the resources:**
    ```bash
    terraform apply
    ```

## Running the Application Locally

1.  **Create and activate a Python virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  **Install dependencies:**
    This `requirements.txt` file includes a version pin for `Werkzeug` to prevent dependency conflicts with Flask.
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set Environment Variables:**
    Load your Spotify credentials and GCS bucket name into your shell session.
    ```bash
    export SPOTIFY_CLIENT_ID=$(grep SPOTIFY_CLIENT_ID .env | cut -d '=' -f2 | tr -d '"' | xargs)
    export SPOTIFY_CLIENT_SECRET=$(grep SPOTIFY_CLIENT_SECRET .env | cut -d '=' -f2 | tr -d '"' | xargs)
    export GCS_BUCKET_NAME=$(grep gcs_bucket_name terraform/terraform.tfvars | cut -d '=' -f2 | tr -d '"' | xargs)
    ```

4.  **Run the Flask app:**
    ```bash
    python app.py
    ```
    The app will be running on `http://localhost:8080`.

## Deployment to Google Cloud Run

Follow these steps to deploy the application as a serverless container.

1.  **Enable Google Cloud APIs:**
    (Terraform now handles enabling Artifact Registry, but it's good practice to ensure these are enabled.)
    ```bash
    gcloud services enable run.googleapis.com cloudbuild.googleapis.com
    ```

2.  **Build and Push the Container Image:**
    This command uses your `terraform.tfvars` file to get the correct repository name.
    ```bash
    export PROJECT_ID=$(grep project_id terraform/terraform.tfvars | cut -d '=' -f2 | tr -d '"' | xargs)
    export REGION=$(grep region terraform/terraform.tfvars | cut -d '=' -f2 | tr -d '"' | xargs)
    export AR_REPO_NAME=$(grep artifact_registry_repo_name terraform/terraform.tfvars | cut -d '=' -f2 | tr -d '"' | xargs)

    gcloud builds submit --tag ${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO_NAME}/spotify-etl-service:latest .
    ```

3.  **Create a Service Account for Cloud Run:**
    ```bash
    gcloud iam service-accounts create spotify-etl-runner \
      --display-name="Service Account for Spotify ETL Cloud Run"

    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
      --member="serviceAccount:spotify-etl-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
      --role="roles/storage.objectAdmin"
    ```

4.  **Deploy to Cloud Run:**
    Replace the placeholders with your actual credentials.
    ```bash
    export SPOTIFY_CLIENT_ID=$(grep SPOTIFY_CLIENT_ID .env | cut -d '=' -f2 | tr -d '"' | xargs)
    export SPOTIFY_CLIENT_SECRET=$(grep SPOTIFY_CLIENT_SECRET .env | cut -d '=' -f2 | tr -d '"' | xargs)
    export GCS_BUCKET_NAME=$(grep gcs_bucket_name terraform/terraform.tfvars | cut -d '=' -f2 | tr -d '"' | xargs)

    gcloud run deploy spotify-etl-service \
      --image ${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO_NAME}/spotify-etl-service:latest \
      --platform managed \
      --region ${REGION} \
      --service-account "spotify-etl-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
      --set-env-vars="^:^SPOTIFY_CLIENT_ID=${SPOTIFY_CLIENT_ID}:SPOTIFY_CLIENT_SECRET=${SPOTIFY_CLIENT_SECRET}:GCS_BUCKET_NAME=${GCS_BUCKET_NAME}" \
      --allow-unauthenticated
    ```

## Usage

### Triggering the ETL Pipeline
Send a GET request to the `/artist/<artist_name>/store` endpoint.

**Local Example:**
```bash
curl "http://localhost:8080/artist/Led%20Zeppelin/store"
```

**Cloud Run Example:**
(Get the URL from the `gcloud run deploy` output)
```bash
SERVICE_URL="https://spotify-etl-service-....run.app"
curl "${SERVICE_URL}/artist/Led%20Zeppelin/store"
```

### Loading Data from GCS to BigQuery
After the ETL job completes, the JSON files will be in your GCS bucket. Use these commands to load them into BigQuery.

```bash
# Set variables from your config file
GCS_BUCKET_NAME=$(grep gcs_bucket_name terraform/terraform.tfvars | cut -d '=' -f2 | tr -d '"' | xargs)
BQ_DATASET_NAME=$(grep bigquery_dataset_name terraform/terraform.tfvars | cut -d '=' -f2 | tr -d '"' | xargs)

# Load Artists
bq load \
  --source_format="NEWLINE_DELIMITED_JSON" \
  --replace \
  "${BQ_DATASET_NAME}.artists" \
  "gs://${GCS_BUCKET_NAME}/artists/*.json"

# Load Albums
bq load \
  --source_format="NEWLINE_DELIMITED_JSON" \
  --replace \
  "${BQ_DATASET_NAME}.albums" \
  "gs://${GCS_BUCKET_NAME}/albums/*.json"

# Load Tracks
bq load \
  --source_format="NEWLINE_DELIMITED_JSON" \
  --replace \
  "${BQ_DATASET_NAME}.tracks" \
  "gs://${GCS_BUCKET_NAME}/tracks/*.json"
```

## Project Structure
```.
├── app.py                  # Main Flask application with all ETL logic.
├── Dockerfile              # Instructions to build the container image.
├── .env                    # Local environment variables (not in git).
├── .git/
├── .gitignore              # Files and folders to be ignored by git.
├── README.md               # This file.
├── requirements.txt        # Python package dependencies.
└── terraform/              # Terraform files for managing cloud infrastructure.
    ├── main.tf
    ├── variables.tf
    ├── terraform.tfvars    # **IMPORTANT**: Your project config lives here.
    ├── outputs.tf
    └── schemas/
        ├── artists_schema.json
        ├── albums_schema.json
        └── tracks_schema.json
```