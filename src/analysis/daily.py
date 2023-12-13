
import os
import sys
import io
import traceback
from typing import Optional, Union, Dict, List
import datetime as dt

import pandas as pd
import boto3
import sqlalchemy as sa

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.module.logging_util import LoadLogger
from src.module.db_connection import PostgreSQL


class LateAnalysis(object):

    def __init__(self,
                 db_connector: object,
                 date: str,
                 table_nm: str,
                 load_file_bucket: str,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None
                 ):

        self._date = date
        self._table_nm = table_nm

        # AWS
        self.__aws_access_key_id = aws_access_key_id
        self.__aws_secret_access_key = aws_secret_access_key
        self._load_file_bucket = load_file_bucket

        if aws_access_key_id and aws_secret_access_key:
            self.__s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.__aws_access_key_id,
                aws_secret_access_key=self.__aws_secret_access_key,
            )
        else:
            self.__s3_client = None

        self.__db_connector = db_connector


        self.__code_table_query = """select station_cd, station_nm, station_id
                                        from station_code sc 
                                        where line ='1호선';
                                         """
        self.__time_table_query = f"""select station_cd, station_nm , substring(train_no,2,5) as train_no, arrive_time, departure_time, start_station_cd , start_station_nm , end_station_cd, end_station_nm , week_tag ,direction ,express_yn ,date
                                        from timetable t 
                                        where date = '{self._date}'
                                        and station_nm != start_station_nm 
                                        and departure_time >= '05:00:00'
                                        order by departure_time ;"""
        # TODO: 날짜 넘어가는 어떤 날짜로 처리할 것인가....

    def get_code_df(self) -> None:
        """
        DB에서 master code 테이블 가져오기

        :return:
        """
        self.code_df = self.__db_connector.sql_dataframe(self.__code_table_query)
        if len(self.code_df) == 0:
            logger.error("code_df is None")

    def get_timetable_df(self) -> None:
        """
        DB에서 열차 시간표 가져오기

        :return:
        """
        self.time_table = self.__db_connector.sql_dataframe(self.__time_table_query)
        if len(self.time_table) == 0:
            logger.error("time_table is None")

        self.time_table['train_no'] = self.time_table['train_no'].apply(int)

    def get_collect_data(self) -> None:
        """
        Spark에서 전처리한 데이터 가져오기

        :return:
        """
        self._s3_find_file()
        parquet_obj = self.__s3_client.get_object(Bucket=self._load_file_bucket, Key=self._s3_file_path)
        self.collect_df = pd.read_parquet(io.BytesIO(parquet_obj["Body"].read()))

        self.collect_df.columns = ['train_no', 'station_id', 'direction', 'express_yn', 'end_station_cd', 'arrival_time', 'departure_time']
        self.collect_df['train_no'] = self.collect_df['train_no'].apply(int)
        self.collect_df = pd.merge(left=self.collect_df, right=self.code_df, left_on='station_id', right_on='station_id', how='left')

    def _s3_find_file(self) -> None:
        object_list = self.__s3_client.list_objects(Bucket=self._load_file_bucket, Prefix=f"position/{self._date}/")['Contents']
        self._file_list = [i['Key'] for i in object_list if "parquet" in i['Key']]
        logger.info(f"S3 File List : {self._file_list}")
        self._s3_file_path = self._file_list[0]

    def join(self) -> None:
        self.merge_df = pd.merge(left=self.collect_df[['train_no', 'station_cd', 'station_id','arrival_time']],
                                 right=self.time_table,
                                 left_on=['train_no', 'station_cd'],
                                 right_on=['train_no', 'station_cd'])

    def calculate(self) -> None:
        self.join()
        self.merge_df = test.merge_df.loc[pd.notnull(self.merge_df['arrival_time'])]
        self.merge_df['arrival_time'] = self.merge_df['arrival_time'].apply(pd.to_datetime)
        self.merge_df['departure_time'] = self.merge_df['date'].apply(str) + " " + self.merge_df['departure_time'].apply(str)
        self.merge_df['departure_time'] = self.merge_df['departure_time'].apply(pd.to_datetime)

        ## 연착 시간 계산
        # 실제 도착 시간 - 예정 출발시간 - 1분(탑승시간)
        self.merge_df['diff_time'] = self.merge_df['arrival_time'] - self.merge_df['departure_time']
        self.merge_df['diff_time'] = self.merge_df['diff_time'].apply(lambda x: int(x.total_seconds())-60)

    def save(self) -> None:
        self.merge_df.rename(
            columns={"arrival_time": "arrival_datetime",
                     "arrive_time": "arrival_time",
                     "diff_time": "delayed_time"},
            inplace=True)

        # 중복 제거
        self.merge_df.drop_duplicates(inplace=True)
        logger.info(f"INSERT DATA COUNT : {len(self.merge_df)}")

        # SQL Alchemy 생성
        self.__db_connector.sa_session()
        self.merge_df.to_sql(name=self._table_nm, con=self.__db_connector.sa_conn, schema="public", if_exists="append", index=False)


if __name__ == '__main__':

    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_database = os.environ['DB_DATABASE']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    aws_access_key_id = os.environ['AWS_ACCESS_KEY']
    aws_secret_access_key = os.environ['AWS_SECRET_KEY']

    logger_class = LoadLogger()
    # logger = logger_class.stream("Test")
    logger = logger_class.time_rotate_file(log_dir="/log/", file_name="anaylsis.log")
    logger.info(
        f"""===========ENV===========\n DB : {db_host} / {db_port} / {db_database} / {db_user} / {db_password} \n ========================="""
    )


    today = dt.datetime.today()
    target_day =  dt.datetime.today() - dt.timedelta(days=1)
    target_day_str = target_day.strftime("%Y-%m-%d")
    logger.info(f"Today : {today}")
    logger.info(f"Target Day (Yesterday) : {target_day_str}")

    try:
        db_conn = PostgreSQL(host=db_host,
                             port=db_port,
                             database=db_database,
                             user=db_user,
                             password=db_password)

        test = LateAnalysis(db_connector=db_conn,
                            date=target_day_str,
                            table_nm="delayed_timetable",
                            load_file_bucket="prj-subway",
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

        logger.info("Load Master Code and Time Table")
        test.get_code_df()
        test.get_timetable_df()

        logger.info("Load Collect Data from S3")
        test.get_collect_data()

        logger.info("calculate Delayed Time")
        test.calculate()

        logger.info("DB INSERT Start")
        test.save()
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())

