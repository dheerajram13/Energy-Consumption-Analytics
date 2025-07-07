"""
Energy Consumption ETL Pipeline

This DAG orchestrates the ETL (Extract, Transform, Load) process for energy consumption data.
The pipeline runs daily and performs the following steps:
1. Extracts energy consumption data from source systems
2. Transforms the data (cleaning, validation, feature engineering)
3. Loads the processed data into the data warehouse
4. Validates the loaded data
5. Generates reports and metrics

Dependencies:
- Python 3.8+
- Apache Airflow 2.0+
- Required Python packages (see requirements.txt)
"""

import logging
from datetime import datetime, timedelta
import pytz
from typing import Dict, Any, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# Import the ETL pipeline function
try:
    # Try the original import path first
    from src.etl.jobs.energy_etl import run_etl_pipeline
except ImportError:
    # Fall back to the mounted project path
    import sys
    sys.path.append('/opt/airflow/energy_analytics')
    from src.etl.jobs.energy_etl import run_etl_pipeline

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args: Dict[str, Any] = {
    'owner': 'energy_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=pytz.UTC),
    'email': Variable.get('alert_email', default_var='alerts@example.com'),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
}

# Get environment-specific variables
ENV = Variable.get('environment', default_var='dev')
SLACK_WEBHOOK = Variable.get('slack_webhook', default_var=None)

# Define the DAG
dag = DAG(
    dag_id=f'energy_consumption_etl_{ENV}',
    default_args=default_args,
    description='ETL pipeline for energy consumption data processing',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
    tags=['energy', 'etl', ENV],
    doc_md=__doc__,
    params={
        'batch_size': 1000,
        'enable_validation': True,
        'enable_anomaly_detection': True,
    },
    on_failure_callback=None,  # Add custom failure callback if needed
    on_success_callback=None,  # Add custom success callback if needed
)

# Slack notification on failure
def notify_failure(context):
    """Send a Slack notification when a task fails."""
    if not SLACK_WEBHOOK:
        return
    
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    
    failed_task = context.get('task_instance').task_id
    exec_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    
    slack_msg = f"""
    :red_circle: Task Failed.
    *Task*: {failed_task}
    *Execution Time*: {exec_date}
    *Log URL*: {log_url}
    """
    
    slack_alert = SlackWebhookOperator(
        task_id='slack_failed',
        webhook_token=SLACK_WEBHOOK,
        message=slack_msg,
        username='airflow',
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    return slack_alert.execute(context=context)

# Define tasks
with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_etl',
        on_failure_callback=notify_failure,
    )
    
    # Main ETL task
    run_etl = PythonOperator(
        task_id='run_energy_etl',
        python_callable=run_etl_pipeline,
        op_kwargs={
            'start_date': '{{ ds }}',
            'end_date': '{{ tomorrow_ds }}',
            'batch_size': '{{ params.batch_size }}',
            'enable_anomaly_detection': '{{ params.enable_anomaly_detection }}',
        },
        on_failure_callback=notify_failure,
    )
    
    # Data validation task
    def validate_data(**kwargs):
        """Validate the loaded data meets quality requirements."""
        ti = kwargs['ti']
        enable_validation = kwargs['params'].get('enable_validation', True)
        
        if not enable_validation:
            logger.info("Data validation is disabled. Skipping...")
            return "skip_validation"

        logger.info("Running data validation...")
        
        # If validation fails, raise an exception
        # if validation_failed:
        #     raise ValueError("Data validation failed")
            
        logger.info("Data validation completed successfully")
        return "validation_passed"
    
    validate_data_task = BranchPythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
        on_failure_callback=notify_failure,
    )
    
    # Skip validation path
    skip_validation = DummyOperator(
        task_id='skip_validation',
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    # Validation passed path
    validation_passed = DummyOperator(
        task_id='validation_passed',
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    # Generate reports task
    def generate_reports(**kwargs):
        """Generate reports and metrics from the processed data."""
        logger.info("Generating reports...")
        # Add your report generation logic here
        logger.info("Reports generated successfully")
    
    reports_task = PythonOperator(
        task_id='generate_reports',
        python_callable=generate_reports,
        on_failure_callback=notify_failure,
    )
    
    # Send success notification
    success_notification = EmailOperator(
        task_id='send_success_email',
        to=default_args['email'],
        subject=f'Energy ETL Success - {datetime.now().strftime("%Y-%m-%d")}',
        html_content="""
        <h3>Energy ETL Pipeline Completed Successfully</h3>
        <p>The daily energy consumption ETL pipeline has completed successfully.</p>
        <p><b>Execution Time:</b> {{ execution_date }}</p>
        <p><b>Environment:</b> {{ var.value.environment }}</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # End task
    end_task = DummyOperator(
        task_id='end_etl',
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    )
    
    # Define task dependencies
    start_task >> run_etl >> validate_data_task
    
    # Branching based on validation result
    validate_data_task >> [validation_passed, skip_validation] >> reports_task
    
    # Final steps
    reports_task >> success_notification >> end_task
