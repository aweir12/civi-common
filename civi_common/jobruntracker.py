import uuid
from datetime import datetime
from datetime import timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructType, StructField, TimestampType
from delta.tables import DeltaTable

class JobRunTracker:
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.job_id = str(uuid.uuid4())
        self.spark = SparkSession.builder.getOrCreate()

    def start_run(self, message: str):
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

        df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("control.job_run")
        print(f"[{self.job_name}] Job started with ID: {self.job_id}")

    def end_run(self, status: str, message: str):
        end_time = datetime.now(timezone.utc)
        self.spark.sql(f"""UPDATE control.job_run
            SET status = '{status}', end_time = timestamp('{end_time}')
            WHERE job_id = '{self.job_id}'""")

        print(f"[{self.job_name}] Job ended with status: {status}")
    
    def update_orphan(self, job_id: str, status: str, message: str):
        end_time = datetime.now(timezone.utc)
        self.spark.sql(f"""UPDATE control.job_run
            SET status = '{status}', end_time = timestamp('{end_time}', message = '{message}'
            WHERE job_id = '{job_id}'""")
        
        print(f"Success, updated [{job_id}]")