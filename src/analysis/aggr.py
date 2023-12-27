
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
    S3에 적재된 데이터를 전처리
    운행편/지하철역/지하철 위처 정보가 중복되는 데이터는 최신 데이터만 저장하고 삭제(30~1분마다 데이터를 수집하는데 지하철의 상태가 동일한 경우가 많음)
    지하철 위치 상태는 접근, 도착, 출발이 있는데, 접근과 출발 데이터의 빈도가 적고, 대부분 도착임. 데이터 분석을 위해 도착 상태의 데이터만 사용

    S3에 json.gz 데이터를 가져와 1개의 parquet 파일로 저장
    """

    def __init__(self, date: str, bucket: str):
        """
        오늘 날짜를 받아 어제, 내일 날짜를 계산 (date를 조작하여 과거 데이터를 수집하기 위한 목적으로도 사용)
        이 코드는 00시 이후에 실행되면, 읽어오는 데이터는 어제자 데이터를 읽어옴

        :param date: "%Y/%m/%d" 형식의 날짜 데이터
        :param bucket: S3 버킷이름
        """
        self.bucket = bucket
        self.today = dt.datetime.strptime(date, "%Y/%m/%d")
        self.today_str = today
        self.today_str_dash = self.today_str.replace("/","-")

        self.yesterday = self.today - dt.timedelta(days=1)
        self.yesterday_str = self.yesterday.strftime("%Y/%m/%d")
        self.yesterday_str_dash = self.yesterday_str.replace("/", "-")

        # self.tomorrow = self.today + dt.timedelta(days=1)
        # self.tomorrow_str = self.tomorrow.strftime("%Y/%m/%d")
        # self.tomorrow_str_dash = self.tomorrow_str.replace("/", "-")

    def create_session(self,
                       name: str,
                       aws_access_key: str,
                       aws_secret_key: str) -> None:
        """
        SPARK Sesstion을 생성
        S3 연결을 위해 key를 입력
        작은 EC2에서 수행되고 있기 때문에 메모리 제한량을 1GB로 설정

        :param name: spark job 이름
        :param aws_access_key: AWS S3 access key
        :param aws_secret_key: AWS S3 secret key
        :return: None
        """
        self.spark = SparkSession.builder.appName(name)\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key)\
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)\
            .config("spark.executor.memory","1GB")\
            .config("spark.drivers.memory","1GB")\
            .getOrCreate()

    def read_data(self) -> None:
        """
        S3에서 데이터를 읽어옴

        :return: NOne
        """
        self.df = self.spark.read.json(f"s3a://{self.bucket}/kinesis/{self.yesterday_str}/*.gz")

    def count(self) -> None:
        """
        읽어온 데이터의 총 개수를 출력
        로그 수집기가 해당 정보를 읽음

        :return: None
        """
        raw_data_count = self.df.count()
        logger.info(f"Raw Data Count : {raw_data_count}")

    def delete_duplicate(self) -> None:
        """
        지하철 상태가 중복되는 데이터를 제거

        :return: None
        """

        # 어제자 데이터만 읽어옴(더블체크용 - 이미 S3에 KST일자별로 파티셔닝되어 있음)
        self.df = self.df.where(f"recptnDt >= '{self.yesterday_str_dash}' and recptnDt < '{self.today_str_dash}'")

        # 상태정보가 완전 동일한 데이터는 제거
        self.distinct_df = self.df.select("trainNo", "statnId", "updnLine", "directAt", "statnTid").distinct()

        # 출발 상태(trainSttus = 2)과 도착 상태(trainSttus = 1)별로 운행편/지하철역별로 최종 수신 시간의 최대값을 뽑아옴(상태정보가 동일하다면 최신 정보를 활용하는 의미)
        self.departure_df = self.df.filter("trainSttus = 2").groupBy("trainNo","statnId").agg(F.max("recptnDt").alias("departure_time")).orderBy("trainNo","statnId")
        self.arrival_df = self.df.filter("trainSttus = 1").groupBy("trainNo","statnId").agg(F.max("recptnDt").alias("arrival_time")).orderBy("trainNo","statnId")

        # 계산된 결과를 종합
        self.distinct_df = self.distinct_df.join(self.arrival_df,['trainNo','statnId'], 'left_outer')
        self.distinct_df = self.distinct_df.join(self.departure_df,['trainNo','statnId'], 'left_outer')

    def save(self) -> None:
        """
        데이터를 S3에 저장

        :return: None
        """
        # 데이터가 매우 크지 않아 1개의 파일로 저장
        self.distinct_df = self.distinct_df.coalesce(1)

        # 정제 완료 후 저장된 데이터의 사이즈를 출력
        saved_data_count = self.distinct_df.count()
        logger.info(f"Saved Data Count : {saved_data_count}")

        # S3에 저장
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