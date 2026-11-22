variable "enable_dataproc_job_logging" {
  type        = bool
  default     = false
  description = "Enable job resource logs at Dataproc cluster creation. Review the plan before enabling on an existing cluster."
}

locals {
  dataproc_properties = merge({
    "dataproc:dataproc.logging.stackdriver.enable"    = "true"
    "dataproc:dataproc.monitoring.stackdriver.enable" = "true"
    "dataproc:jobs.file-backed-output.enable"         = "true"
    "dataproc:dataproc.allow.zero.workers"            = local.capacity.flink_workers == 0 ? "true" : "false"
    }, var.enable_dataproc_job_logging ? {
    "dataproc:dataproc.logging.stackdriver.job.driver.enable"         = "true"
    "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable" = "true"
  } : {})
}

# Adapted from iac-integration. Prometheus and Grafana remain managed by Argo CD.
variable "alert_email" {
  type        = string
  default     = ""
  description = "Optional Cloud Monitoring email destination."
}

variable "alert_notification_channels" {
  type        = list(string)
  default     = []
  description = "Existing Cloud Monitoring notification channels, if any."
}

resource "google_monitoring_notification_channel" "email" {
  count        = var.alert_email == "" ? 0 : 1
  display_name = "${var.prefix} alert email"
  type         = "email"

  labels = {
    email_address = var.alert_email
  }
}

locals {
  alert_channels = concat(var.alert_notification_channels, google_monitoring_notification_channel.email[*].id)
}

resource "google_monitoring_alert_policy" "sql_cpu" {
  display_name          = "${var.prefix}: Cloud SQL CPU above 80%"
  combiner              = "OR"
  notification_channels = local.alert_channels

  conditions {
    display_name = "Cloud SQL CPU utilization"

    condition_threshold {
      filter          = "resource.type = \"cloudsql_database\" AND resource.labels.database_id = \"${var.project_id}:${google_sql_database_instance.database.name}\" AND metric.type = \"cloudsql.googleapis.com/database/cpu/utilization\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0.8
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "gke_node_cpu" {
  display_name          = "${var.prefix}: GKE node CPU above 85%"
  combiner              = "OR"
  notification_channels = local.alert_channels

  conditions {
    display_name = "GKE node CPU allocatable utilization"

    condition_threshold {
      filter          = "resource.type = \"k8s_node\" AND resource.labels.cluster_name = \"${google_container_cluster.gke.name}\" AND metric.type = \"kubernetes.io/node/cpu/allocatable_utilization\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0.85
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "notification_function_errors" {
  display_name          = "${var.prefix}: notification function returning errors"
  combiner              = "OR"
  notification_channels = local.alert_channels

  conditions {
    display_name = "Cloud Run 5xx responses for the notification function"

    condition_threshold {
      filter          = "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = \"${google_cloudfunctions2_function.notification.name}\" AND metric.type = \"run.googleapis.com/request_count\" AND metric.labels.response_code_class = \"5xx\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_monitoring_dashboard" "platform" {
  dashboard_json = jsonencode({
    displayName = "${var.prefix} platform"
    mosaicLayout = {
      columns = 12
      tiles = [
        {
          width  = 6
          height = 4
          widget = {
            title = "Cloud SQL CPU utilization"
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter      = "resource.type = \"cloudsql_database\" AND metric.type = \"cloudsql.googleapis.com/database/cpu/utilization\""
                    aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MEAN" }
                  }
                }
              }]
            }
          }
        },
        {
          xPos   = 6
          width  = 6
          height = 4
          widget = {
            title = "GKE node CPU allocatable utilization"
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter      = "resource.type = \"k8s_node\" AND resource.labels.cluster_name = \"${google_container_cluster.gke.name}\" AND metric.type = \"kubernetes.io/node/cpu/allocatable_utilization\""
                    aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MEAN" }
                  }
                }
              }]
            }
          }
        },
        {
          yPos   = 4
          width  = 12
          height = 4
          widget = {
            title = "Notification function requests by response class"
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter      = "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = \"${google_cloudfunctions2_function.notification.name}\" AND metric.type = \"run.googleapis.com/request_count\""
                    aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_RATE", groupByFields = ["metric.label.response_code_class"], crossSeriesReducer = "REDUCE_SUM" }
                  }
                }
              }]
            }
          }
        },
        {
          yPos   = 8
          width  = 6
          height = 4
          widget = {
            title = "CPU request utilization of the autoscaled services (HPA target 60%)"
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter      = "resource.type = \"k8s_container\" AND resource.labels.cluster_name = \"${google_container_cluster.gke.name}\" AND resource.labels.pod_name = monitoring.regex.full_match(\"(customer|order)-service-.*\") AND metric.type = \"kubernetes.io/container/cpu/request_utilization\""
                    aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MEAN", groupByFields = ["resource.label.pod_name"], crossSeriesReducer = "REDUCE_MEAN" }
                  }
                }
              }]
            }
          }
        },
        {
          xPos   = 6
          yPos   = 8
          width  = 6
          height = 4
          widget = {
            title = "Running pods: customer-service and order-service (scales out under load)"
            xyChart = {
              dataSets = [
                {
                  legendTemplate = "customer-service"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter      = "resource.type = \"k8s_container\" AND resource.labels.cluster_name = \"${google_container_cluster.gke.name}\" AND resource.labels.pod_name = monitoring.regex.full_match(\"customer-service-.*\") AND metric.type = \"kubernetes.io/container/uptime\""
                      aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MAX", crossSeriesReducer = "REDUCE_COUNT" }
                    }
                  }
                },
                {
                  legendTemplate = "order-service"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter      = "resource.type = \"k8s_container\" AND resource.labels.cluster_name = \"${google_container_cluster.gke.name}\" AND resource.labels.pod_name = monitoring.regex.full_match(\"order-service-.*\") AND metric.type = \"kubernetes.io/container/uptime\""
                      aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_MAX", crossSeriesReducer = "REDUCE_COUNT" }
                    }
                  }
                },
              ]
            }
          }
        },
        {
          yPos   = 12
          width  = 12
          height = 4
          widget = {
            title = "Error log lines: microservices and the Flink analytics cluster (from Cloud Logging)"
            xyChart = {
              dataSets = [
                {
                  legendTemplate = "microservices"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter      = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.service_errors.name}\""
                      aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_RATE", crossSeriesReducer = "REDUCE_SUM" }
                    }
                  }
                },
                {
                  legendTemplate = "analytics (Dataproc)"
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter      = "metric.type = \"logging.googleapis.com/user/${google_logging_metric.analytics_errors.name}\""
                      aggregation = { alignmentPeriod = "60s", perSeriesAligner = "ALIGN_RATE", crossSeriesReducer = "REDUCE_SUM" }
                    }
                  }
                },
              ]
            }
          }
        },
      ]
    }
  })

  depends_on = [google_project_service.apis]
}

# Log-based metrics: Cloud Logging is the centralized log store for every workload.
# GKE ships all container logs and the Dataproc cluster ships its job logs (see gke.tf, dataproc.tf).
resource "google_logging_metric" "service_errors" {
  name   = "${var.prefix}_service_error_logs"
  filter = "resource.type = \"k8s_container\" AND resource.labels.cluster_name = \"${google_container_cluster.gke.name}\" AND resource.labels.namespace_name = \"food\" AND severity >= ERROR"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }

  depends_on = [google_project_service.apis]
}

resource "google_logging_metric" "analytics_errors" {
  name   = "${var.prefix}_analytics_error_logs"
  filter = "resource.type = \"cloud_dataproc_job\" AND resource.labels.job_id =~ \"^food-analytics-[0-9a-f]{20}$\" AND severity >= ERROR"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }

  depends_on = [google_project_service.apis]
}
