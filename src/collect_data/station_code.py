
import os
import time
from typing import Union, Optional, Dict, List
import datetime as dt

import psycopg2
import requests as req
import pandas as pd

from src.module.api_call import api_status_check
from src.module.db_connection import PostgreSQL


class CollectStationCode(object):

    def __init__(self):
        self.operation_info_df = None
        self.stations_df = None

    @staticmethod
    @api_status_check
    def __request_api(url: str):
        return req.get(url)

    def get_station_info(self):
        result_json = self.__request_api(
            url='https://pts.map.naver.com/cs-pc-subway/api/subway/lanes/1/stations?caller=pc_search')

        self.operation_info_df = pd.DataFrame([{'operation_id': i['id'], 'operation_name': i['longName'], 'line': '1호선'}
                                               for i in
                                               result_json])

        stations = []
        for data in result_json:
            stations += data['stations']
        self.stations_df = pd.DataFrame(stations).drop_duplicates().reset_index(drop=True)
        self.stations_df.rename(columns={'id': 'station_id',
                                         'name': 'station_name',
                                         'displayName': 'long_station_name'},
                                inplace=True)


if __name__ == '__main__':
    station_info = CollectStationCode()
    station_info.get_station_info()
    station_df = station_info.stations_df
    operation_df=station_info.operation_info_df

    station_df.to_csv("./data/naver_staton_code.csv", index=False)
    operation_df.to_csv("./data/naver_operation_info.csv", index=False)