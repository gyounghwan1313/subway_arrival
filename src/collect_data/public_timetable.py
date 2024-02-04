
import os
import sys
from typing import Optional, Union, Dict, List
import time
import datetime as dt
import traceback
import requests as rq

import pandas as pd
import requests as req

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.module.logging_util import LoadLogger
from src.module.api_status_check import api_status_check
from src.module.db_connection import PostgreSQL
from src.module.s3_util import S3Client


class CollectPublicTimeTable(object):

    def __init__(self,
                 key: str,
                 db_connector: object,
                 table_name: str,
                 aws_access_key: Optional[str] = None,
                 aws_secret_key: Optional[str] = None):
        self.__key = key
        self.__db_connector = db_connector
        self.__query = """select case when length(station_cd) <4 then rpad('0',4, station_cd)
                                else station_cd 
                                end as station_cd
                                , station_nm 
                            from station_code sc 
                            where line = '1호선'
                            order by station_cd 
                            ;"""
        self.code_df = self.load_data_from_db(self.__query)
        self.table_name = table_name
        self.today = dt.datetime.today().strftime("%Y-%m-%d")
        self.__week_type_str = ["평일", "평일", "평일", "평일", "평일", "토요일", "일요일"][dt.datetime.today().weekday()]
        self.__week_type_code = 1 if self.__week_type_str == "평일" else 2 if self.__week_type_str == "토요일" else 3

        self.total_df = None
        if aws_access_key and aws_secret_key:
            self.__aws_access_key = aws_access_key
            self.__aws_secret_key = aws_secret_key

    def load_data_from_db(self, query: str) -> Optional[pd.DataFrame]:
        code_df = self.__db_connector.sql_dataframe(query)

        return code_df

    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        code_df = self.__db_connector.sql_execute(query)

        return code_df

    @api_status_check
    def api_request(self, url: str) -> Optional[req.Response]:
        """
        API 요청 - GET 방식

        :param url: 요청 URL
        :return: API 요청 결과 오브젝트
        """
        self._now = dt.datetime.now()
        logger.info(f" time : {self._now}")
        self._result = req.get(url=url)

        return self._result

    def call(self, url, call_count=0) -> Optional[Dict]:
        if call_count >= 2:
            return None

        self._get_data_json = self.api_request(url=url)

        if self._get_data_json:
            if "SearchSTNTimeTableByIDService" not in self._get_data_json:
                logger.info("==ERROR==")
                return None
            elif "RESULT" not in self._get_data_json['SearchSTNTimeTableByIDService'] and "CODE" not in self._get_data_json['SearchSTNTimeTableByIDService']["RESULT"]:
                logger.info("==Code key is Not Found")
                return None
            else:
                self._get_data_json = self._get_data_json['SearchSTNTimeTableByIDService']
                return self._get_data_json
        else:  # 응답값이 없으면 : 에러가 발생함 -> 2번 더 호출하고 실패하면 멈춤
            logger.info("Retry")
            logger.info(f"{call_count+1}")
            time.sleep(10)
            return self.call(url, call_count + 1)

    def transform(self, data_key: str) -> pd.DataFrame:
        self.raw_data_df = pd.DataFrame(self._get_data_json[data_key])
        self.raw_data_df.columns = [i.lower() for i in self.raw_data_df.columns]
        self.raw_data_df.replace("01호선", "1호선", inplace=True)
        self.raw_data_df.rename(columns={"line_num": "line",
                                         "arrivetime": "arrive_time",
                                         "lefttime": "departure_time",
                                         "originstation": "start_station_cd",
                                         "deststation": "end_station_cd",
                                         "subwaysname": "start_station_nm",
                                         "subwayename": "end_station_nm",
                                         "inout_tag": "direction",
                                         }, inplace=True)
        self.raw_data_df['date'] = self.today
        self.raw_data_df['arrive_time']=self.raw_data_df['arrive_time'].apply(lambda x: f"{int(x.split(':')[0])-24}:{x.split(':')[1]}:{x.split(':')[2]}" if int(x.split(':')[0])>=24 else x)
        self.raw_data_df['departure_time']=self.raw_data_df['departure_time'].apply(lambda x: f"{int(x.split(':')[0])-24}:{x.split(':')[1]}:{x.split(':')[2]}" if int(x.split(':')[0])>=24 else x)

        return self.raw_data_df

    def insert_to_db(self, df):
        insert_query = []
        for idx, data in df.iterrows():
            insert_query.append(f"""INSERT INTO {self.table_name}
                                   (line, fr_code, station_cd, station_nm, train_no, arrive_time, departure_time, start_station_cd, end_station_cd, start_station_nm, end_station_nm, week_tag, direction, express_yn, branch_line, date)
                                   VALUES ('{data['line']}','{data['fr_code']}','{data['station_cd']}', '{data['station_nm']}', '{data['train_no']}', '{data['arrive_time']}', '{data['departure_time']}', '{data['start_station_cd']}', '{data['end_station_cd']}', '{data['start_station_nm']}','{data['end_station_nm']}', '{data['week_tag']}','{data['direction']}','{data['express_yn']}','{data['branch_line']}', '{data['date']}')
                                              ;""")

        self.execute_query(" \n".join(insert_query))

    def delete_data(self, code, date):
        self.__db_connector.sql_execute(
            f"""delete from {self.table_name} where station_cd = '{code}' and date = '{date}';""")

    def upload_to_s3(self,
                     df: pd.DataFrame,
                     bucket: str,
                     local_file_path: str,
                     s3_file_path: str) -> None:
        df.to_parquet(local_file_path)
        s3_client = S3Client(aws_access_key=self.__aws_access_key, aws_secret_key=self.__aws_secret_key)
        s3_client.upload_object(bucket=bucket, local_file_path=local_file_path, s3_file_path=s3_file_path, delete=True)

    def run(self):
        logger.info("""===== DATA Collect START =====""")
        for idx, value in self.code_df.iterrows():
            code = value['station_cd']
            name = value['station_nm']
            logger.info(f"[{idx+1}/{len(self.code_df)}] {name}({code}) ")

            self.delete_data(code=code, date=self.today)
            self.call(url=f"http://openAPI.seoul.go.kr:8088/{self.__key}/json/SearchSTNTimeTableByIDService/1/1000/{code}/{self.__week_type_code}/1/")
            df_1 = self.transform(data_key='row')
            self.total_df = pd.concat([self.total_df, df_1])
            self.insert_to_db(df=self.raw_data_df)

            self.call(url=f"http://openAPI.seoul.go.kr:8088/{self.__key}/json/SearchSTNTimeTableByIDService/1/1000/{code}/{self.__week_type_code}/2/")
            df_2 = self.transform(data_key='row')
            self.total_df = pd.concat([self.total_df, df_2])
            self.insert_to_db(df=self.raw_data_df)
            time.sleep(1)

        self.upload_to_s3(df=self.total_df,
                          bucket="prj-subway",
                          local_file_path=f"/data/timetable/{self.today}.parquet",
                          s3_file_path=f"timetable/{self.today}.parquet")





if __name__ == '__main__':

    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_database = os.environ['DB_DATABASE']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    key = os.environ["api_key"]
    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']

    logger_class = LoadLogger()
    logger = logger_class.time_rotate_file(log_dir="/log/", file_name=f"timetable.log")
    logger.info(
        f"""===========ENV===========\n Key : {key} \n DB : {db_host} / {db_port} / {db_database} / {db_user} / {db_password}  / {aws_access_key} / {aws_secret_key} \n ========================="""
    )

    db_conn = PostgreSQL(host=db_host,
                         port=db_port,
                         database=db_database,
                         user=db_user,
                         password=db_password)

    try:
        timetable = CollectPublicTimeTable(key=key,
                                           db_connector=db_conn,
                                           table_name="timetable",
                                           aws_access_key=aws_access_key,
                                           aws_secret_key=aws_secret_key)
        timetable.run()
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        for k, v in timetable.__dict__.items():
            logger.error("== timetable Class ==")
            logger.error("Key : ", k)
            logger.error("Value : ", v)
