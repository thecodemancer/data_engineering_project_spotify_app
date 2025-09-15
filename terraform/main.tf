# main.tf

# Configure the Google Cloud provider
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable the Artifact Registry API
resource "google_project_service" "artifactregistry" {
  service = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

# 1. Create the Artifact Registry Docker Repository
# This resource depends on the API being enabled first.
resource "google_artifact_registry_repository" "docker_repo" {
  provider      = google
  location      = var.region
  repository_id = var.artifact_registry_repo_name
  description   = "Docker repository for the Spotify ETL application"
  format        = "DOCKER"

  # Ensure the API is enabled before trying to create the repo
  depends_on = [google_project_service.artifactregistry]
}

# 2. Create the Regional GCS Bucket for Staging Data
resource "google_storage_bucket" "staging_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  # This rule applies to OBJECTS within the bucket (e.g., your JSON files)
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = var.staging_file_ttl_days
    }
  }

  lifecycle {
    prevent_destroy = false # Allows 'terraform destroy' to delete the bucket
  }
}


# 3. Create the BigQuery Dataset in the same region as the GCS Bucket
resource "google_bigquery_dataset" "spotify_dataset" {
  dataset_id                  = var.bigquery_dataset_name
  friendly_name               = "Spotify Data"
  description                 = "Dataset containing artists, albums, and tracks from Spotify"
  location                    = var.region
  delete_contents_on_destroy  = false
}


# 4. Create the BigQuery Tables with pre-defined schemas
resource "google_bigquery_table" "artists_table" {
  dataset_id = google_bigquery_dataset.spotify_dataset.dataset_id
  table_id   = "artists"
  schema     = file("schemas/artists_schema.json")
}

resource "google_bigquery_table" "albums_table" {
  dataset_id = google_bigquery_dataset.spotify_dataset.dataset_id
  table_id   = "albums"
  schema     = file("schemas/albums_schema.json")
# Temporarily disable deletion protection to allow recreation
  deletion_protection = false   
}

resource "google_bigquery_table" "tracks_table" {
  dataset_id = google_bigquery_dataset.spotify_dataset.dataset_id
  table_id   = "tracks"
  schema     = file("schemas/tracks_schema.json")

  deletion_protection = false     
}