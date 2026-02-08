terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
#  credentials = 
  project = "data-engineer-camp-2026"
  region  = "africa-south1"
}



resource "google_storage_bucket" "data-lake-bucket" {
  name          = "data-lake-bucket-2026"
  location      = "africa-south1" # This is the ID for Johannesburg
  force_destroy = true

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "data_engineer_camp_2026_dataset"
  project    = "data-engineer-camp-2026"
  location   = "africa-south1"
  delete_contents_on_destroy = true
}