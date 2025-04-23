import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, TimestampType
from delta.tables import DeltaTable

class JobRunTracker:
    def __init__(self, job_name: str, auto_start: bool = False, start_message: str = None):
        self.job_name = job_name
        self.job_id = str(uuid.uuid4())
        self.spark = SparkSession.builder.getOrCreate()

        if auto_start:
            self.start_run(start_message)

    def start_run(self, message: str = None):
        start_time = datetime.now(timezone.utc)

        schema = StructType([
            StructField("job_id", StringType(), False),
            StructField("job_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("message", StringType(), True)
        ])

        data = [(self.job_id, self.job_name, "Running", start_time, None, message)]
        df = self.spark.createDataFrame(data, schema=schema)

        df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("cntl_job_run")
        print(f"[{self.job_name}] Job started with ID: {self.job_id}")

    def end_run(self, status: str, message: str = None):
        end_time = datetime.now(timezone.utc)
        update_query = f"""
            UPDATE cntl_job_run
            SET status = '{status}', end_time = timestamp('{end_time}'){"," if message else ""}
            {"message = '" + message + "'" if message else ""}
            WHERE job_id = '{self.job_id}'
        """
        self.spark.sql(update_query)
        print(f"[{self.job_name}] Job ended with status: {status}")
    
    def update_orphan(self, job_id: str, status: str, message: str = None):
        end_time = datetime.now(timezone.utc)
        update_query = f"""
            UPDATE cntl_job_run
            SET status = '{status}', end_time = timestamp('{end_time}'){"," if message else ""}
            {"message = '" + message + "'" if message else ""}
            WHERE job_id = '{job_id}'
        """
        self.spark.sql(update_query)
        print(f"Success, updated [{job_id}]")
