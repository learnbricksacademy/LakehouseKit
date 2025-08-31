# ============================================================
# Databricks Job for Orchestrator Notebook
# Runs Landing → Raw → Harmonize → Refined pipeline
# ============================================================

resource "databricks_job" "pipeline_orchestrator" {
  name = "Pipeline Orchestrator"

  task {
    task_key = "orchestrator"
    notebook_task {
      notebook_path = "/Repos/databricks-data-migration-framework/notebooks/orchestrator/run_pipeline.py"
      base_parameters = {
        pipeline_stage = "all"
      }
    }
    existing_cluster_id = var.cluster_id   # or use a job_cluster block if you want jobs-only cluster
  }

  email_notifications {
    on_failure = ["team@datacompany.com"]
    on_success = []
  }

  max_concurrent_runs = 1
}
