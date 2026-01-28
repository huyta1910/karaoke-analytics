from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime
from config import Config

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=Config.BQ_PROJECT_ID)
        self.dataset_ref = f"{Config.BQ_PROJECT_ID}.{Config.BQ_DATASET_ID}"
        self.table_ref = f"{self.dataset_ref}.{Config.BQ_TABLE_ID}"
        self._ensure_setup()

    def _ensure_setup(self):
        """Creates Dataset if it doesn't exist."""
        try:
            self.client.get_dataset(self.dataset_ref)
        except NotFound:
            print(f"Creating dataset {self.dataset_ref}...")
            ds = bigquery.Dataset(self.dataset_ref)
            ds.location = "US"
            self.client.create_dataset(ds)

    def get_last_ingested_date(self):
        """
        State Management: Queries BQ to find the latest date we already have.
        Returns: datetime object or None (if table is empty).
        """
        query = f"""
            SELECT MAX(date) as last_date 
            FROM `{self.table_ref}`
        """
        try:
            query_job = self.client.query(query)
            results = list(query_job.result())
            if results and results[0].last_date:
                return results[0].last_date
            return None
        except NotFound:
            return None 

    def get_last_post_date(self):
        """
        Get the latest post created_at date from BigQuery.
        Returns: datetime object or None (if table is empty/doesn't exist).
        """
        table_ref = f"{self.dataset_ref}.{Config.BQ_POSTS_TABLE_ID}"
        query = f"""
            SELECT MAX(created_at) as last_date 
            FROM `{table_ref}`
        """
        try:
            query_job = self.client.query(query)
            results = list(query_job.result())
            if results and results[0].last_date:
                return results[0].last_date
            return None
        except NotFound:
            return None

    def upload_data(self, df):
        if df is None or df.empty:
            return

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("metric_name", "STRING"),
                bigquery.SchemaField("value", "INTEGER"),
                bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
            ],
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND", 
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date"
            )
        )

        try:
            job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
            job.result()
            print(f"Uploaded {len(df)} rows to BigQuery.")
        except Exception as e:
            print(f"Upload Failed: {e}")
            
    def upload_posts_data(self, df):
        if df is None or df.empty:
            return

        table_ref = f"{self.dataset_ref}.{Config.BQ_POSTS_TABLE_ID}"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("post_id", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
                bigquery.SchemaField("message", "STRING"),
                bigquery.SchemaField("post_url", "STRING"),
                bigquery.SchemaField("impressions", "INTEGER"),
                
                bigquery.SchemaField("engaged_users", "INTEGER"),
                
                bigquery.SchemaField("likes", "INTEGER"),
                bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
            ],
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND", 
        )

        try:
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            print(f"Uploaded {len(df)} recent posts to {Config.BQ_POSTS_TABLE_ID}.")
        except Exception as e:
            print(f"Post Upload Failed: {e}")