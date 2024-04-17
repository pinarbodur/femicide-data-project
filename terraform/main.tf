terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.24.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "femicide_data_project_bucket" {
  name          = var.bucket_name
  location      = var.region
  storage_class = var.bucket_storage_class
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "femicide_tr_dataset" {
  dataset_id = var.dataset_id
  location   = var.region
}