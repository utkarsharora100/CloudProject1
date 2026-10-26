terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

variable "project_id" {
  description = "The ID of the Google Cloud project"
  type        = string
  default     = "cloud-project-512416"
}

variable "region" {
  description = "The region to deploy resources in"
  type        = string
  default     = "us-central1"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. Enable Required APIs
resource "google_project_service" "services" {
  for_each = toset([
    "compute.googleapis.com",
    "servicenetworking.googleapis.com",
    "sqladmin.googleapis.com",
    "secretmanager.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# 2. VPC and Private Subnet Setup
resource "google_compute_network" "private_network" {
  name                    = "food-delivery-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.services]
}

resource "google_compute_subnetwork" "private_subnet" {
  name          = "food-delivery-subnet"
  ip_cidr_range = "10.0.1.0/24"
  region        = var.region
  network       = google_compute_network.private_network.id
}

# 3. Private Services Access for Cloud SQL
resource "google_compute_global_address" "private_ip_address" {
  name          = "cloud-sql-private-ip-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.private_network.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.private_network.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

# 4. Google Secret Manager for Database Password
resource "random_password" "db_password" {
  length           = 24
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "google_secret_manager_secret" "db_pass_secret" {
  secret_id = "customer-db-password"
  replication {
    auto {}
  }
  depends_on = [google_project_service.services]
}

resource "google_secret_manager_secret_version" "db_pass_version" {
  secret      = google_secret_manager_secret.db_pass_secret.id
  secret_data = random_password.db_password.result
}

# 5. Production Cloud SQL Instance
resource "google_sql_database_instance" "mysql_instance" {
  name             = "food-delivery-prod-db"
  database_version = "MYSQL_8_0"
  region           = var.region
  depends_on       = [google_service_networking_connection.private_vpc_connection]

  settings {
    tier              = "db-custom-2-7680" # Production-grade sizing (2 vCPU, 7.5GB RAM)
    availability_type = "REGIONAL"         # Cross-zone high availability

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.private_network.id
    }

    backup_configuration {
      enabled                        = true
      start_time                     = "02:00"
      binary_log_enabled = true
    }
  }
  
  # Set to true for real production, left false here to allow clean teardown for the assignment
  deletion_protection = false 
}

# 6. Database and User
resource "google_sql_database" "customer_db" {
  name     = "customers_db"
  instance = google_sql_database_instance.mysql_instance.name
}

resource "google_sql_user" "db_user" {
  name     = "customer_service_user"
  instance = google_sql_database_instance.mysql_instance.name
  password = random_password.db_password.result
}

resource "google_sql_database" "orders_db" {
  name     = "orders_db"
  instance = google_sql_database_instance.mysql_instance.name
}

output "mysql_private_ip_address" {
  description = "The private IPv4 address of the database instance"
  value       = google_sql_database_instance.mysql_instance.private_ip_address
}

output "db_password_secret_name" {
  description = "The Secret Manager resource name holding the DB password"
  value       = google_secret_manager_secret.db_pass_secret.name
}