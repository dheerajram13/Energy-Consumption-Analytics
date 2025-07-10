# dags/energy_etl_dag.py - Complete working DAG
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import os
import pandas as pd

# Add project path
sys.path.append('/opt/airflow/energy_analytics')

from src.etl.extractors.smart_meter_extractor import SmartMeterExtractor
from src.etl.transformers.energy_transformer import EnergyConsumptionTransformer
from src.etl.loaders.database import DatabaseLoader
from src.config.database import get_db_session

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    'energy_consumption_etl',
    default_args=default_args,
    description='Energy Analytics ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['energy', 'etl', 'analytics']
)

def extract_energy_data(**context):
    """Extract energy consumption data"""
    execution_date = context['execution_date']
    start_date = execution_date.strftime('%Y-%m-%d')
    end_date = (execution_date + timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Initialize extractor
    extractor = SmartMeterExtractor(simulate_data=True)
    
    # Extract data
    df = extractor.extract(start_date, end_date, num_meters=50)
    
    # Save to temporary location
    output_path = f'/tmp/energy_data_{execution_date.strftime("%Y%m%d")}.csv'
    df.to_csv(output_path, index=False)
    
    print(f"Extracted {len(df)} records to {output_path}")
    return output_path

def transform_energy_data(**context):
    """Transform energy consumption data"""
    ti = context['task_instance']
    input_path = ti.xcom_pull(task_ids='extract_data')
    
    # Read extracted data
    df = pd.read_csv(input_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Initialize transformer
    transformer = EnergyConsumptionTransformer()
    
    # Transform data
    data_dicts = df.to_dict('records')
    transformed_data = transformer.transform(data_dicts)
    
    # Save transformed data
    output_path = input_path.replace('.csv', '_transformed.csv')
    transformed_df = pd.DataFrame(transformed_data)
    transformed_df.to_csv(output_path, index=False)
    
    print(f"Transformed {len(transformed_data)} records")
    return output_path

def load_energy_data(**context):
    """Load energy consumption data to database"""
    ti = context['task_instance']
    input_path = ti.xcom_pull(task_ids='transform_data')
    
    # Read transformed data
    df = pd.read_csv(input_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Convert to list of dicts
    data_dicts = df.to_dict('records')
    
    # Initialize loader
    db_session = next(get_db_session())
    loader = DatabaseLoader(db_session)
    
    # Load data
    stats = loader.load(data_dicts)
    
    print(f"Load completed: {stats}")
    return stats

def run_anomaly_detection(**context):
    """Run anomaly detection on loaded data"""
    from src.ml.energy_anomaly_detector import EnergyAnomalyDetector
    
    execution_date = context['execution_date']
    
    # Get data from database for anomaly detection
    db_session = next(get_db_session())
    
    # Query recent data (last 7 days)
    from src.models.energy_models import EnergyConsumption
    
    query = db_session.query(EnergyConsumption).filter(
        EnergyConsumption.timestamp >= execution_date - timedelta(days=7),
        EnergyConsumption.timestamp <= execution_date
    )
    
    df = pd.read_sql(query.statement, db_session.bind)
    
    if len(df) > 24:  # Need enough data for training
        # Initialize and train anomaly detector
        detector = EnergyAnomalyDetector(method='isolation_forest', contamination=0.1)
        detector.fit(df)
        
        # Detect anomalies
        results = detector.predict(df)
        
        # Save anomalies to database
        anomalies = results[results['is_anomaly'] == True]
        
        from src.models.energy_models import Anomaly
        
        for _, row in anomalies.iterrows():
            anomaly = Anomaly(
                timestamp=row['timestamp'],
                region=row['region'],
                actual_value=row['consumption_mwh'],
                predicted_value=0,  # Would need prediction model
                anomaly_score=row['anomaly_score'],
                is_confirmed=0
            )
            db_session.add(anomaly)
        
        db_session.commit()
        
        print(f"Detected {len(anomalies)} anomalies")
    
    db_session.close()

def cleanup_temp_files(**context):
    """Clean up temporary files"""
    execution_date = context['execution_date']
    import glob
    
    # Remove temporary files
    pattern = f'/tmp/energy_data_{execution_date.strftime("%Y%m%d")}*.csv'
    files = glob.glob(pattern)
    
    for file in files:
        try:
            os.remove(file)
            print(f"Removed {file}")
        except:
            pass

# Define tasks
check_db = PostgresOperator(
    task_id='check_database',
    postgres_conn_id='postgres_default',
    sql="SELECT 1;",
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_energy_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_energy_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_energy_data,
    dag=dag
)

anomaly_task = PythonOperator(
    task_id='detect_anomalies',
    python_callable=run_anomaly_detection,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_temp_files,
    trigger_rule='all_done',
    dag=dag
)

# Define dependencies
check_db >> extract_task >> transform_task >> load_task >> anomaly_task >> cleanup_task
