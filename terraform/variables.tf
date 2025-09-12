# variables.tf

variable "project_id" {
  type        = string
  description = "The Google Cloud project ID to deploy resources into."
}

variable "region" {
  type        = string
  description = "The Google Cloud region for all resources (e.g., 'us-central1')."
}

variable "gcs_bucket_name" {
  type        = string
  description = "The globally unique name for the GCS bucket."
}

variable "bigquery_dataset_name" {
  type        = string
  description = "The name for the BigQuery dataset."
}

variable "staging_file_ttl_days" {
  type        = number
  description = "Number of days after which to delete files from the GCS staging bucket."
}

variable "artifact_registry_repo_name" {
  type        = string
  description = "The name for the Artifact Registry repository."
}