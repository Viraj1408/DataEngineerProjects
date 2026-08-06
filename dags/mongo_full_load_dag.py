from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag_name = 'mongo_full_load_dag'
start_date = datetime(2026, 7, 17)
l_interval_schedule = "30 6 * * *"
mongo_uri = 'mongodb+srv://<username>:<password>@cluster.mongodb.net'
default_args = {
    "owner": "data-eng",
    "retries": 1,
}

mem = {
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "spark.cores.max": "2",
        }

with DAG(
    dag_id = dag_name,
    start_date = start_date,
    schedule = l_interval_schedule,
    catchup=False   
) as dag:
    extract_users = SparkSubmitOperator(
        task_id="extract_users",
        application="/jobs/mongo_full_load.py",
        conn_id="spark_default",
        application_args=[
            "--mongo_uri", mongo_uri,
            "--database", "sample_mflix",
            "--collection", "users",
            "--output_path", "/data/delta/bronze/sample_mflix_users_full",
            "--schema_path", "/jobs/schema/users_schema.py"
        ],

        conf=mem
    )

    extract_comments = SparkSubmitOperator(
        task_id="extract_comments",
        application="/jobs/mongo_full_load.py",
        conn_id="spark_default",
        application_args=[
            "--mongo_uri", mongo_uri,
            "--database", "sample_mflix",
            "--collection", "comments",
            "--output_path", "/data/delta/bronze/sample_mflix_comments_full",
            "--schema_path", "NA"
        ],

        conf=mem
    )

    extract_movies = SparkSubmitOperator(
        task_id="extract_movies",
        application="/jobs/mongo_full_load.py",
        conn_id="spark_default",
        application_args=[
            "--mongo_uri", mongo_uri,
            "--database", "sample_mflix",
            "--collection", "movies",
            "--output_path", "/data/delta/bronze/sample_mflix_movies_full",
            "--schema_path", "NA"
        ],

        conf=mem
    )

extract_users >> extract_comments >> extract_movies

# Parallel Execution
# extract_users
# extract_comments
# extract_movies
