locals {
  jobs = jsondecode(file("${path.module}/../jobs.json"))
}

resource "databricks_job" "pipeline_jobs" {
  for_each = { for job in local.jobs.jobs : job.name => job }

  name = each.value.name

  dynamic "notebook_task" {
    for_each = try(each.value.notebook_task, [])
    content {
      notebook_path = notebook_task.value["notebook_path"]
    }
  }

  dynamic "spark_python_task" {
    for_each = try(each.value.spark_python_task, [])
    content {
      python_file = spark_python_task.value["python_file"]
    }
  }

  schedule {
    quartz_cron_expression = each.value.schedule.cron
    timezone_id            = "UTC"
  }
}
