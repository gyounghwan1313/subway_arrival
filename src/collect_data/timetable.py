
import os
import sys
import time
import traceback
from typing import Union, Optional, Dict, List
import datetime as dt

import psycopg2
import requests as req
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.module.logging_util import LoadLogger
from src.module.api_status_check import api_status_check
from src.module.db_connection import PostgreSQL


class CollectSubwayTimeTable(object):

    def __init__(self,
                 db_connector: object,
                 table_name: str):

        self.__db_connector = db_connector
        self.__query = """select station_id, station_name from naver_station_code nsc ;"""
        self.code_df = self.load_data_from_db(self.__query)
        self.table_name = table_name
        self.today = dt.datetime.today().strftime("%Y-%m-%d")

    # 코드 정보 가져옴

    def load_data_from_db(self, query: str) -> Optional[pd.DataFrame]:
        code_df = self.__db_connector.sql_dataframe(query)

        return code_df


    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        code_df = self.__db_connector.sql_execute(query)

        return code_df

    @staticmethod
    @api_status_check
    def request_api(url: str):
        return req.get(url)

    def get_timetable(self):
        for idx, data in self.code_df.iterrows():
            code = data['station_id']
            logger.info("{code}, {data['station_name']}")
            result_json = self.request_api(
                f"https://pts.map.naver.com/cs-pc-subway/api/subway/stations/{code}/schedule?date=&caller=pc_search")

            self.up_df = pd.DataFrame(result_json['weekdaySchedule']['up'])
            self.down_df = pd.DataFrame(result_json['weekdaySchedule']['down'])
            if len(self.up_df) > 0:
                self.up_df['direction'] = 'up'
            if len(self.down_df) > 0:
                self.down_df['direction'] = 'down'

            self.df = pd.concat([self.up_df, self.down_df])


            self.df['operation'] = self.df['operation'].apply(lambda x: x['type'])
            self.df['station_id'] = code
            self.df.rename(columns={"departureTime": "departure_time",
                                       "startStation": "start_station",
                                       "headsign": "head_sign",
                                       "operation": "operation",
                                       "operationOrder": "operation_order",
                                       "laneId": "lane_id",
                                       "lastTrip": "is_last_trip",
                                       "firstTrip": "is_first_trip"
                                       }, inplace=True)

            self.df['is_last_trip'] = self.df['is_last_trip'].apply(lambda x: '1' if x else '0')
            self.df['is_first_trip'] = self.df['is_first_trip'].apply(lambda x: '1' if x else '0')
            self.df['date'] = self.today

            self.delete_data(code=code, date=self.today)
            self.save()
            time.sleep(2.2)
            if idx % 10 == 0:
                time.sleep(3.23)

    def insert_to_db(self, df):
        for idx, data in df.iterrows():
            insert_query = f"""INSERT INTO {self.table_name}
                                (station_id, departure_time, start_station, head_sign, operation, operation_order, lane_id, is_last_trip, is_first_trip, direction, date)
                                VALUES ('{data['station_id']}','{data['departure_time']}', '{data['start_station']}', '{data['head_sign']}', '{data['operation']}', '{data['operation_order']}', '{data['lane_id']}', '{data['is_last_trip']}', '{data['is_first_trip']}', '{data['direction']}', '{data['date']}');"""
            self.execute_query(insert_query)

    def delete_data(self, code, date):
        self.__db_connector.sql_execute(f"""delete from {self.table_name} where station_id = '{code}' and date = '{date}';""")

    def save(self):
        self.insert_to_db(self.df)


if __name__ == '__main__':

    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_database = os.environ['DB_DATABASE']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']


    logger_class = LoadLogger()
    logger = logger_class.time_rotate_file(log_dir="/log/", file_name=f"timetable.log")

    try:
        db_conn = PostgreSQL(host=db_host,
                             port=db_port,
                             database=db_database,
                             user=db_user,
                             password=db_password)
        collect_timetable = CollectSubwayTimeTable(db_conn, table_name="station_timetable")
        collect_timetable.get_timetable()
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        for k, v in collect_timetable.__dict__:
            logger.error(f"{k}: {v}")
        sys.exit(1)
