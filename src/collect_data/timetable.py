import os
import time
from typing import Union, Optional, Dict, List
import datetime as dt

import psycopg2
import requests as req
import pandas as pd

from src.module.api_call import api_status_check
from src.module.db_connection import PostgreSQL


class CollectSubwayTimeTable(object):

    def __init__(self):

        self.__query = """select substring(station_id,8,3) as station_id, station_name 
                    from station_code sc
                    where line = '1호선'
                    order by station_id asc;"""
        self.code_df = self.load_data_from_db(self.__query)

    # 코드 정보 가져옴
    @staticmethod
    def load_data_from_db(query: str) -> Optional[pd.DataFrame]:
        db_connector = PostgreSQL(host='subway-arrial-postgresql.crpljesaooh0.ap-northeast-2.rds.amazonaws.com',
                                  port=5432,
                                  database='subway_arrival',
                                  user='postgres',
                                  password='thfgmlrudghks0808!')

        code_df = db_connector.sql_dataframe(query)

        del db_connector
        return code_df

    @staticmethod
    def execute_query(query: str) -> Optional[pd.DataFrame]:
        db_connector = PostgreSQL(host='subway-arrial-postgresql.crpljesaooh0.ap-northeast-2.rds.amazonaws.com',
                                  port=5432,
                                  database='subway_arrival',
                                  user='postgres',
                                  password='thfgmlrudghks0808!')

        code_df = db_connector.sql_execute(query)

        del db_connector
        return code_df

    @staticmethod
    @api_status_check
    def request_api(url: str):
        return req.get(url)

    def get_timetable(self):
        for idx, data in self.code_df.iterrows():
            code = data['station_id']
            print(code, data['station_name'])
            result_json = self.request_api(
                f"https://pts.map.naver.com/cs-pc-subway/api/subway/stations/{code}/schedule?date=&caller=pc_search")

            self.up_df = pd.DataFrame(result_json['weekdaySchedule']['up'])
            self.down_df = pd.DataFrame(result_json['weekdaySchedule']['down'])
            if len(self.up_df) > 1:
                self.up_df['operation'] = self.up_df['operation'].apply(lambda x: x['type'])
                self.up_df.rename(columns={"departureTime": "departure_time",
                                           "startStation": "start_station",
                                           "headsign": "head_sign",
                                           "operation": "operation",
                                           "operationOrder": "operation_order",
                                           "laneId": "lane_id",
                                           "lastTrip": "is_last_trip",
                                           "firstTrip": "is_first_trip"
                                           }, inplace=True)
                self.up_df['direction'] = 'up'
                self.up_df['is_last_trip'] = self.up_df['is_last_trip'].apply(lambda x: '1' if x else '0')
                self.up_df['is_first_trip'] = self.up_df['is_first_trip'].apply(lambda x: '1' if x else '0')
                self.up_df['date'] = dt.datetime.today().strftime("%Y-%m-%d")

            if len(self.down_df) > 1:
                self.down_df['operation'] = self.down_df['operation'].apply(lambda x: x['type'].lower())
                self.down_df.rename(columns={"departureTime": "departure_time",
                                             "startStation": "start_station",
                                             "headsign": "head_sign",
                                             "operation": "operation",
                                             "operationOrder": "operation_order",
                                             "laneId": "lane_id",
                                             "lastTrip": "is_last_trip",
                                             "firstTrip": "is_first_trip"
                                             }, inplace=True)
                self.down_df['direction'] = 'down'
                self.down_df['is_last_trip'] = self.down_df['is_last_trip'].apply(lambda x: '1' if x else '0')
                self.down_df['is_first_trip'] = self.down_df['is_first_trip'].apply(lambda x: '1' if x else '0')
                self.down_df['date'] = dt.datetime.today().strftime("%Y-%m-%d")

            time.sleep(2.2)
            if idx % 10 == 0:
                time.sleep(3.23)

    def insert_to_db(self, df, table_name):
        for idx, data in df.iterrows():
            upsert_query = f"""INSERT INTO {table_name}
                                (departure_time, start_station, head_sign, operation, operation_order, lane_id, is_last_trip, is_first_trip, direction, date)
                                VALUES ('{data['departure_time']}', '{data['start_station']}', '{data['head_sign']}', '{data['operation']}', '{data['operation_order']}', '{data['lane_id']}', '{data['is_last_trip']}', '{data['is_first_trip']}', '{data['direction']}', '{data['date']}')
                                ON CONFLICT (date, operation_order)
                                DO UPDATE 
                                SET departure_time='{data['departure_time']}', start_station='{data['start_station']}', head_sign='{data['head_sign']}', operation='{data['operation']}', lane_id='{data['lane_id']}', is_last_trip='{data['is_last_trip']}', is_first_trip='{data['is_first_trip']}', direction='{data['direction']}';"""
            self.execute_query(upsert_query)

    def save(self, table_name):
        if len(self.up_df) > 0:
            self.insert_to_db(self.up_df, table_name)
        if len(self.down_df) > 0:
            self.insert_to_db(self.down_df, table_name)


if __name__ == '__main__':
    collect_timetable = CollectSubwayTimeTable()
    collect_timetable.get_timetable()
    collect_timetable.save(table_name="station_timetable")
