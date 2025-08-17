# =============================================================================
# Berlin BnB Pipeline - Infrastructure
# =============================================================================

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 3.5"
    }
  }
}

# Configure the Google Provider
provider "google" {
    credentials = file(var.credentials_file)
    project     = var.project_id
    region      = var.region
}


resource "google_storage_bucket" "raw_data_bucket" {
  name          = var.raw_data_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id    = var.bq_dataset_name
}