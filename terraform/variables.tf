variable "credentials_file" {
  description = "Path to the Google Cloud service account key file."
  type        = string
  default     = "./keys/berlinbnb-60035e5dca95.json"
  
}

variable "project_id" {
  description = "The Google Cloud project ID."
  type        = string
  default     = "berlinbnb"
  
}

variable "region" {
  description = "The Google Cloud region."
  type        = string
  default     = "us-central1"
  
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset to be created."
  type        = string
  default     = "berlinbnb_dataset"
  
}

variable "raw_data_bucket_name" {
  description = "The name of the Google Cloud Storage bucket for raw data."
  type        = string
  default     = "berlinbnb_raw_data_bucket"
}

variable "location" {
  description = "The location for the Google Cloud Storage bucket."
  type        = string
  default     = "US"
  
}