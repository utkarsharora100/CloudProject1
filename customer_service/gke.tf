# gke.tf

# 1. GKE Zonal Cluster Definition
resource "google_container_cluster" "primary" {
  name     = "food-delivery-cluster"
  # Changed from region to a specific zone to prevent 3x duplication
  location = "us-central1-a" 

  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.private_network.name
  subnetwork = google_compute_subnetwork.private_subnet.name

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
}

# 2. Managed Node Pool with reduced storage footprint
resource "google_container_node_pool" "primary_nodes" {
  name       = "food-delivery-node-pool"
  location   = "us-central1-a"
  cluster    = google_container_cluster.primary.name
  node_count = 2 # Creates exactly 2 nodes in one zone

  node_config {
    machine_type = "e2-medium"
    
    # Force standard HDD and smaller sizes to avoid SSD quota limits
    disk_type    = "pd-standard" 
    disk_size_gb = 30           

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    
    labels = {
      environment = "production"
    }
  }
}

output "kubernetes_cluster_name" {
  value       = google_container_cluster.primary.name
  description = "GKE Cluster Name"
}

output "kubernetes_cluster_host" {
  value       = google_container_cluster.primary.endpoint
  description = "GKE Cluster Host"
}