import logging
import sys
import os

import datetime as dt
import traceback

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.module.logging_util import LoadLogger


class Aggr(object):

    def __init__(self, date: str, bucket):
        self.bucket = bucket
        self.date = date

    def create_session(self, name, aws_access_key, aws_secret_key):
        self.spark = SparkSession.builder.appName(name)\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key)\
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)\
            .config("spark.executor.memory","1GB")\
            .config("spark.drivers.memory","1GB")\
            .getOrCreate()

    def read_data(self,):
        self.df = self.spark.read.json(f"s3a://{self.bucket}/topics/subway-position-topic/{self.date}/*.json.gz")

    def count(self):
        self.df.count()

    def delete_duplicate(self):
        self.distinct_df = self.df.select("trainNo", "statnId", "updnLine", "directAt", "statnTid").distinct()
        self.departure_df = self.df.filter("trainSttus = 2").groupBy("trainNo","statnId").agg(F.max("recptnDt").alias("departure_time")).orderBy("trainNo","statnId")
        self.arrival_df = self.df.filter("trainSttus = 1").groupBy("trainNo","statnId").agg(F.max("recptnDt").alias("arrival_time")).orderBy("trainNo","statnId")
        self.distinct_df = self.distinct_df.join(self.arrival_df,['trainNo','statnId'], 'left_outer')
        self.distinct_df = self.distinct_df.join(self.departure_df,['trainNo','statnId'], 'left_outer')

    def save(self):
        self.distinct_df.write.mode("overwrite").parquet(f"s3a://{self.bucket}/position/{self.date}/")


if __name__ == '__main__':

    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']

    logger_class = LoadLogger()
    logger = logger_class.time_rotate_file(log_dir="/log/", file_name=f"spark_aggr.log")

    today = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        spark_run = Aggr(date=today, bucket="prj-subway")
        logger.info("==== Create SPARK Session ====")
        spark_run.create_session(name="aggr", aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
        logger.info("==== READ DATA ====")
        spark_run.read_data()
        logger.info("==== DATA COUNT ====")
        spark_run.count()
        logger.info("==== DATA Preprocessing ====")
        spark_run.delete_duplicate()
        logger.info("==== DATA SAVA to S3 ====")
        spark_run.save()
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        sys.exit(1)