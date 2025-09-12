# outputs.tf

output "gcs_bucket_name" {
  value       = google_storage_bucket.staging_bucket.name
  description = "The name of the created GCS bucket."
}

output "bigquery_dataset_id" {
  value       = google_bigquery_dataset.spotify_dataset.dataset_id
  description = "The ID of the created BigQuery dataset."
}

output "bigquery_table_ids" {
  value = {
    artists = google_bigquery_table.artists_table.table_id
    albums  = google_bigquery_table.albums_table.table_id
    tracks  = google_bigquery_table.tracks_table.table_id
  }
  description = "The IDs of the created BigQuery tables."
}

output "artifact_registry_repository_url" {
  value       = "${google_artifact_registry_repository.docker_repo.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}"
  description = "The full URL of the Artifact Registry Docker repository."
}