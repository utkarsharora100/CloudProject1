mock_provider "confluent" {}

variables {
  gcp_project_id = "assignment-test-123"
  prefix         = "food"
}

run "dedicated_basic_cluster_and_scoped_runtime" {
  command = plan

  assert {
    condition     = confluent_kafka_cluster.food.display_name == "food-kafka" && length(confluent_kafka_cluster.food.basic) == 1
    error_message = "The experiment must plan a dedicated Basic Kafka cluster."
  }

  assert {
    condition     = length(confluent_kafka_acl.app) == 15
    error_message = "Expected eight topic ACLs, four consumer groups, idempotence and two transaction permissions."
  }

  assert {
    condition = alltrue([
      for acl in confluent_kafka_acl.app :
      acl.permission == "ALLOW" && (acl.pattern_type == "LITERAL" ||
      (acl.resource_type == "TRANSACTIONAL_ID" && acl.resource_name == "restaurant-analytics-v1-"))
    ])
    error_message = "Only the application transaction prefix may use a prefix ACL."
  }

  assert {
    condition     = confluent_service_account.app.display_name != confluent_service_account.admin.display_name
    error_message = "The runtime and topic administrator must be separate service accounts."
  }
}
