
import sys
import os

import datetime as dt
import traceback

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.module.logging_util import LoadLogger


class Aggr(object):
    """
    date

    """

    def __init__(self, date: str, bucket: str):
        self.bucket = bucket
        self.today = dt.datetime.strptime(date, "%Y/%m/%d")
        self.today_str = today
        self.today_str_dash = self.today_str.replace("/","-")

        self.yesterday = self.today - dt.timedelta(days=1)
        self.yesterday_str = self.yesterday.strftime("%Y/%m/%d")
        self.yesterday_str_dash = self.yesterday_str.replace("/", "-")

        self.tomorrow = self.today + dt.timedelta(days=1)
        self.tomorrow_str = self.tomorrow.strftime("%Y/%m/%d")
        self.tomorrow_str_dash = self.tomorrow_str.replace("/", "-")

    def create_session(self, name: str, aws_access_key: str, aws_secret_key: str) -> None:
        self.spark = SparkSession.builder.appName(name)\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key)\
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)\
            .config("spark.executor.memory","1GB")\
            .config("spark.drivers.memory","1GB")\
            .getOrCreate()

    def read_data(self) -> None:
        self.df = self.spark.read.json(f"s3a://{self.bucket}/kinesis/{self.yesterday_str}/*.gz")

    def count(self) -> None:
        count = self.df.count()
        logger.info(f"Raw Data Count : {count}")

    def delete_duplicate(self) -> None:
        self.df = self.df.where(f"recptnDt >= '{self.yesterday_str_dash}' and recptnDt < '{self.today_str_dash}'")
        self.distinct_df = self.df.select("trainNo", "statnId", "updnLine", "directAt", "statnTid").distinct()
        self.departure_df = self.df.filter("trainSttus = 2").groupBy("trainNo","statnId").agg(F.max("recptnDt").alias("departure_time")).orderBy("trainNo","statnId")
        self.arrival_df = self.df.filter("trainSttus = 1").groupBy("trainNo","statnId").agg(F.max("recptnDt").alias("arrival_time")).orderBy("trainNo","statnId")
        self.distinct_df = self.distinct_df.join(self.arrival_df,['trainNo','statnId'], 'left_outer')
        self.distinct_df = self.distinct_df.join(self.departure_df,['trainNo','statnId'], 'left_outer')

    def save(self) -> None:
        self.distinct_df.write.mode("overwrite").parquet(f"s3a://{self.bucket}/position/{self.yesterday_str_dash}/")


if __name__ == '__main__':

    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']

    logger_class = LoadLogger()
    logger = logger_class.time_rotate_file(log_dir="/log/", file_name=f"spark_aggr.log")

    today = (dt.datetime.today()).strftime("%Y/%m/%d")

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