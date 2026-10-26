terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "1.51.0"
    }
  }
}

# You must provide these credentials via environment variables:
# export CONFLUENT_CLOUD_API_KEY="<your-api-key>"
# export CONFLUENT_CLOUD_API_SECRET="<your-api-secret>"
provider "confluent" {}

resource "confluent_environment" "food_delivery_env" {
  display_name = "food-delivery-production"
}

# Provision a Basic Kafka cluster hosted on GCP
resource "confluent_kafka_cluster" "main_cluster" {
  display_name = "food-delivery-kafka"
  availability = "SINGLE_ZONE"
  cloud        = "GCP"
  region       = "us-central1"
  basic {}
  environment {
    id = confluent_environment.food_delivery_env.id
  }
}

# Create a Service Account for the microservices
resource "confluent_service_account" "app_manager" {
  display_name = "food-delivery-microservices"
  description  = "Service account for Order and Kitchen services"
}

# Bind the Role to the Service Account
resource "confluent_role_binding" "app_manager_binding" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main_cluster.rbac_crn
}

# Generate API Keys for the microservices to connect
resource "confluent_api_key" "app_manager_kafka_api_key" {
  display_name = "app-manager-kafka-api-key"
  description  = "Kafka API Key for microservices"
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.main_cluster.id
    api_version = confluent_kafka_cluster.main_cluster.api_version
    kind        = confluent_kafka_cluster.main_cluster.kind
    environment {
      id = confluent_environment.food_delivery_env.id
    }
  }
  depends_on = [confluent_role_binding.app_manager_binding]
}

# Create Kafka Topics
resource "confluent_kafka_topic" "orders" {
  kafka_cluster {
    id = confluent_kafka_cluster.main_cluster.id
  }
  topic_name       = "orders"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.main_cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager_kafka_api_key.id
    secret = confluent_api_key.app_manager_kafka_api_key.secret
  }
}

resource "confluent_kafka_topic" "kitchen_status" {
  kafka_cluster {
    id = confluent_kafka_cluster.main_cluster.id
  }
  topic_name       = "kitchen_status"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.main_cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager_kafka_api_key.id
    secret = confluent_api_key.app_manager_kafka_api_key.secret
  }
}

output "kafka_bootstrap_servers" {
  value = confluent_kafka_cluster.main_cluster.bootstrap_endpoint
}

output "kafka_api_key" {
  value     = confluent_api_key.app_manager_kafka_api_key.id
  sensitive = true
}

output "kafka_api_secret" {
  value     = confluent_api_key.app_manager_kafka_api_key.secret
  sensitive = true
}