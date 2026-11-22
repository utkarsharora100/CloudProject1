terraform {
  required_version = ">= 1.6.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.50.0"
    }
  }

  backend "gcs" {}
}

# Supply short-lived management credentials through the provider environment.
provider "confluent" {}

variable "gcp_project_id" {
  type        = string
  description = "GCP project whose deployment this Kafka cluster serves."
}

variable "prefix" {
  type = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,14}$", var.prefix))
    error_message = "Use the same deployment prefix as deploy/config.team.json."
  }
}

variable "cloud" {
  type    = string
  default = "AWS"
  validation {
    condition     = contains(["AWS", "GCP", "AZURE"], var.cloud)
    error_message = "Select a supported Confluent cloud provider."
  }
}

variable "region" {
  type    = string
  default = "us-east-2"
}

resource "confluent_environment" "food" {
  display_name = "${var.prefix}-env"

  stream_governance {
    package = "ESSENTIALS"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_kafka_cluster" "food" {
  display_name = "${var.prefix}-kafka"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region

  basic {}

  environment {
    id = confluent_environment.food.id
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "confluent_service_account" "app" {
  display_name = "${var.prefix}-app"
  description  = "Reads and writes only the food delivery application resources."
}

resource "confluent_service_account" "admin" {
  display_name = "${var.prefix}-topic-admin"
  description  = "Manages topics and ACLs within the dedicated food delivery cluster."
}

resource "confluent_role_binding" "admin" {
  principal   = "User:${confluent_service_account.admin.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.food.rbac_crn
}

resource "confluent_api_key" "app" {
  display_name = "${var.prefix}-app-key"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.food.id
    api_version = confluent_kafka_cluster.food.api_version
    kind        = confluent_kafka_cluster.food.kind

    environment {
      id = confluent_environment.food.id
    }
  }
}

resource "confluent_api_key" "admin" {
  display_name = "${var.prefix}-topic-admin-key"

  owner {
    id          = confluent_service_account.admin.id
    api_version = confluent_service_account.admin.api_version
    kind        = confluent_service_account.admin.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.food.id
    api_version = confluent_kafka_cluster.food.api_version
    kind        = confluent_kafka_cluster.food.kind

    environment {
      id = confluent_environment.food.id
    }
  }

  depends_on = [confluent_role_binding.admin]
}

locals {
  topic_permissions = {
    orders                       = ["READ", "WRITE"]
    kitchen_status               = ["READ", "WRITE"]
    delivery_assignments         = ["READ", "WRITE"]
    restaurant_analytics_results = ["READ", "WRITE"]
  }
  consumer_groups = [
    "kitchen-workflow-group",
    "notification-service-v1",
    "restaurant-analytics-v1",
    "restaurant-analytics-results-v1",
  ]
  topic_acls = flatten([
    for topic, operations in local.topic_permissions : [
      for operation in operations : {
        type = "TOPIC", name = topic, pattern = "LITERAL", operation = operation
      }
    ]
  ])
  app_acls = concat(local.topic_acls, [
    for group in local.consumer_groups : {
      type = "GROUP", name = group, pattern = "LITERAL", operation = "READ"
    }
    ], [
    { type = "CLUSTER", name = "kafka-cluster", pattern = "LITERAL", operation = "IDEMPOTENT_WRITE" },
    { type = "TRANSACTIONAL_ID", name = "restaurant-analytics-v1-", pattern = "PREFIXED", operation = "WRITE" },
    { type = "TRANSACTIONAL_ID", name = "restaurant-analytics-v1-", pattern = "PREFIXED", operation = "DESCRIBE" },
  ])
}

resource "confluent_kafka_acl" "app" {
  for_each = { for acl in local.app_acls : "${acl.type}:${acl.name}:${acl.operation}" => acl }

  kafka_cluster {
    id = confluent_kafka_cluster.food.id
  }

  resource_type = each.value.type
  resource_name = each.value.name
  pattern_type  = each.value.pattern
  principal     = "User:${confluent_service_account.app.id}"
  host          = "*"
  operation     = each.value.operation
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.food.rest_endpoint

  credentials {
    key    = confluent_api_key.admin.id
    secret = confluent_api_key.admin.secret
  }
}

# deploy.managed_kafka reads this directly without echoing the result.
output "deployment" {
  sensitive = true

  value = {
    gcp_project_id      = var.gcp_project_id
    prefix              = var.prefix
    kafka_cluster_id    = confluent_kafka_cluster.food.id
    kafka_rest_endpoint = confluent_kafka_cluster.food.rest_endpoint
    kafka_bootstrap     = trimprefix(confluent_kafka_cluster.food.bootstrap_endpoint, "SASL_SSL://")
    application = {
      kafka_api_key    = confluent_api_key.app.id
      kafka_api_secret = confluent_api_key.app.secret
    }
    topic_admin = {
      kafka_api_key    = confluent_api_key.admin.id
      kafka_api_secret = confluent_api_key.admin.secret
    }
  }

  depends_on = [confluent_kafka_acl.app]
}
