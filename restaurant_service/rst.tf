terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80"
    }
  }
}

variable "project_id" {
  description = "The ID of the Google Cloud project"
  type        = string
  default     = "cloud-project-512416"
}

provider "google" {
  project = var.project_id
}

# Provision Google Cloud Firestore in Native Mode
resource "google_firestore_database" "database" {
  project     = var.project_id
  name        = "(default)"
  location_id = "nam5" # US multi-region for high availability
  type        = "FIRESTORE_NATIVE"
}