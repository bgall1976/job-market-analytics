"""
Job Analytics Pipeline DAG
==========================

Orchestrates the complete data pipeline:
1. Scrape job postings from multiple sources
2. Process raw data (Bronze → Silver)
3. Run dbt transformations (Silver → Gold)
4. Validate data quality
5. Update AWS Glue catalog
6. Refresh dashboard
7. Send notifications

Schedule: Daily at 6:00 AM UTC
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule
import os
import json


# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Environment variables
S3_BRONZE_BUCKET = os.environ.get("S3_BRONZE_BUCKET", "job-analytics-bronze-xxxx")
S3_SILVER_BUCKET = os.environ.get("S3_SILVER_BUCKET", "job-analytics-silver-xxxx")
S3_GOLD_BUCKET = os.environ.get("S3_GOLD_BUCKET", "job-analytics-gold-xxxx")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# Paths
SCRAPER_PATH = "/opt/airflow/scrapers"
DBT_PATH = "/opt/airflow/dbt"
GX_PATH = "/opt/airflow/great_expectations"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def send_slack_notification(context: Dict[str, Any], status: str = "info"):
    """Send Slack notification for task status."""
    
    if not SLACK_WEBHOOK_URL:
        print("Slack webhook not configured, skipping notification")
        return
    
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    
    emoji = {
        "success": "✅",
        "failure": "❌",
        "info": "ℹ️"
    }.get(status, "📌")
    
    message = {
        "text": f"{emoji} *{dag_id}* - {task_id}",
        "attachments": [
            {
                "color": "#36a64f" if status == "success" else "#ff0000",
                "fields": [
                    {"title": "Status", "value": status.upper(), "short": True},
                    {"title": "Execution Date", "value": str(execution_date), "short": True}
                ]
            }
        ]
    }
    
    import requests
    requests.post(SLACK_WEBHOOK_URL, json=message)


def on_failure_callback(context):
    """Callback for task failures."""
    send_slack_notification(context, "failure")


def on_success_callback(context):
    """Callback for DAG success."""
    send_slack_notification(context, "success")


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="job_analytics_pipeline",
    description="Daily job market analytics pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["job-analytics", "production"],
    doc_md=__doc__,
    on_failure_callback=on_failure_callback,
) as dag:

    # =========================================================================
    # TASK GROUPS
    # =========================================================================

    @task_group(group_id="ingestion")
    def ingestion_tasks():
        """Data ingestion task group."""
        
        @task(task_id="scrape_rapidapi")
        def scrape_rapidapi():
            """Scrape jobs from RapidAPI."""
            import subprocess
            result = subprocess.run(
                ["python", f"{SCRAPER_PATH}/job_scraper.py", 
                 "--source", "rapidapi",
                 "--output", f"s3://{S3_BRONZE_BUCKET}/job_postings/rapidapi/"],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise Exception(f"Scraper failed: {result.stderr}")
            return {"source": "rapidapi", "status": "success"}
        
        @task(task_id="scrape_greenhouse")
        def scrape_greenhouse():
            """Scrape jobs from Greenhouse boards."""
            import subprocess
            result = subprocess.run(
                ["python", f"{SCRAPER_PATH}/job_scraper.py",
                 "--source", "greenhouse", 
                 "--output", f"s3://{S3_BRONZE_BUCKET}/job_postings/greenhouse/"],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise Exception(f"Scraper failed: {result.stderr}")
            return {"source": "greenhouse", "status": "success"}
        
        @task(task_id="scrape_lever")
        def scrape_lever():
            """Scrape jobs from Lever boards."""
            import subprocess
            result = subprocess.run(
                ["python", f"{SCRAPER_PATH}/job_scraper.py",
                 "--source", "lever",
                 "--output", f"s3://{S3_BRONZE_BUCKET}/job_postings/lever/"],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise Exception(f"Scraper failed: {result.stderr}")
            return {"source": "lever", "status": "success"}
        
        # Run scrapers in parallel
        [scrape_rapidapi(), scrape_greenhouse(), scrape_lever()]
    
    @task_group(group_id="transformation")
    def transformation_tasks():
        """dbt transformation task group."""
        
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_PATH} && dbt deps",
        )
        
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"cd {DBT_PATH} && dbt run --profiles-dir {DBT_PATH}",
        )
        
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PATH} && dbt test --profiles-dir {DBT_PATH}",
        )
        
        dbt_deps >> dbt_run >> dbt_test
    
    @task_group(group_id="quality")
    def quality_tasks():
        """Data quality validation task group."""
        
        @task(task_id="validate_silver")
        def validate_silver():
            """Run Great Expectations on Silver layer."""
            import subprocess
            result = subprocess.run(
                ["python", f"{GX_PATH}/../scripts/run_validations.py",
                 "--checkpoint", "silver_checkpoint"],
                capture_output=True,
                text=True
            )
            return {
                "success": result.returncode == 0,
                "output": result.stdout
            }
        
        @task(task_id="validate_gold")
        def validate_gold():
            """Run Great Expectations on Gold layer."""
            import subprocess
            result = subprocess.run(
                ["python", f"{GX_PATH}/../scripts/run_validations.py",
                 "--checkpoint", "gold_checkpoint"],
                capture_output=True,
                text=True
            )
            return {
                "success": result.returncode == 0,
                "output": result.stdout
            }
        
        [validate_silver(), validate_gold()]

    # =========================================================================
    # MAIN TASKS
    # =========================================================================

    start = EmptyOperator(task_id="start")
    
    # Processing task (Bronze → Silver)
    process_bronze_silver = BashOperator(
        task_id="process_bronze_silver",
        bash_command="""
            echo "Processing Bronze to Silver..."
            # In production, this would trigger Databricks job
            # For local testing, run PySpark script directly
            python /opt/airflow/processing/01_bronze_to_silver.py \
                --bronze-path s3://${S3_BRONZE_BUCKET}/job_postings/ \
                --silver-path s3://${S3_SILVER_BUCKET}/jobs_cleaned/
        """,
        env={
            "S3_BRONZE_BUCKET": S3_BRONZE_BUCKET,
            "S3_SILVER_BUCKET": S3_SILVER_BUCKET,
        }
    )
    
    # Glue crawler tasks
    crawl_silver = GlueCrawlerOperator(
        task_id="crawl_silver_layer",
        crawler_name="job-analytics-silver-crawler",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )
    
    crawl_gold = GlueCrawlerOperator(
        task_id="crawl_gold_layer",
        crawler_name="job-analytics-gold-crawler",
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )
    
    # Dashboard refresh (placeholder)
    refresh_dashboard = BashOperator(
        task_id="refresh_dashboard",
        bash_command="""
            echo "Refreshing Streamlit dashboard..."
            # In production, trigger dashboard cache refresh
            curl -X POST http://streamlit:8501/refresh || true
        """,
    )
    
    # Summary notification
    @task(task_id="send_summary", trigger_rule=TriggerRule.ALL_DONE)
    def send_summary(**context):
        """Send pipeline summary notification."""
        
        # Gather stats
        execution_date = context["execution_date"]
        
        summary = {
            "text": "📊 *Job Analytics Pipeline Complete*",
            "attachments": [
                {
                    "color": "#36a64f",
                    "fields": [
                        {"title": "Execution Date", "value": str(execution_date), "short": True},
                        {"title": "Status", "value": "Success", "short": True},
                    ]
                }
            ]
        }
        
        if SLACK_WEBHOOK_URL:
            import requests
            requests.post(SLACK_WEBHOOK_URL, json=summary)
        
        return summary
    
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # =========================================================================
    # TASK DEPENDENCIES
    # =========================================================================
    
    # Main pipeline flow
    start >> ingestion_tasks() >> process_bronze_silver >> transformation_tasks()
    transformation_tasks() >> quality_tasks() >> [crawl_silver, crawl_gold]
    [crawl_silver, crawl_gold] >> refresh_dashboard >> send_summary() >> end


# =============================================================================
# MAINTENANCE DAG
# =============================================================================

with DAG(
    dag_id="job_analytics_maintenance",
    description="Weekly maintenance tasks",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * 0",  # Weekly on Sunday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["job-analytics", "maintenance"],
) as maintenance_dag:
    
    cleanup_old_data = BashOperator(
        task_id="cleanup_old_data",
        bash_command="""
            # Delete files older than 90 days
            aws s3 ls s3://${S3_BRONZE_BUCKET}/job_postings/ --recursive | \
            awk '{print $4}' | \
            while read file; do
                file_date=$(echo $file | grep -oP '\d{8}')
                if [ -n "$file_date" ]; then
                    days_old=$(( ($(date +%s) - $(date -d $file_date +%s)) / 86400 ))
                    if [ $days_old -gt 90 ]; then
                        echo "Deleting old file: $file"
                        # aws s3 rm s3://${S3_BRONZE_BUCKET}/$file
                    fi
                fi
            done
        """,
        env={"S3_BRONZE_BUCKET": S3_BRONZE_BUCKET}
    )
    
    vacuum_database = BashOperator(
        task_id="vacuum_database",
        bash_command=f"cd {DBT_PATH} && dbt run-operation vacuum_tables"
    )
    
    rebuild_data_docs = BashOperator(
        task_id="rebuild_data_docs",
        bash_command=f"cd {GX_PATH} && great_expectations docs build"
    )
    
    cleanup_old_data >> vacuum_database >> rebuild_data_docs
