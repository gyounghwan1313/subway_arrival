
from typing import Optional, Dict
import os
import time
import datetime as dt
import logging
import boto3

import pandas as pd
import requests as req

from src.module.api_status_check import api_status_check


# 도착 일괄 : http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimeStationArrival/ALL
# 실시간 위치 :"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimePosition/0/100/1호선

class CollectPublicData(object):

    def __init__(self,
                 aws_access_key_id: Optional[str]=None,
                 aws_secret_access_key: Optional[str]=None):

        self._get_data = None
        self._pd_data = None
        self._now = None

        if aws_access_key_id or aws_secret_access_key:
            self.__aws_access_key_id = aws_access_key_id
            self.__aws_secret_access_key = aws_secret_access_key
            self.__s3_client = None
        else:
            self.__s3_client = boto3.client("s3",
                                            aws_access_key_id=self.__aws_access_key_id,
                                            aws_secret_access_key=self.__aws_secret_access_key)

    @api_status_check
    def api_request(self,url):
        self._now = dt.datetime.now()
        print(self._now)
        # self._get_data = req.get(url=url)
        return req.get(url=url)

    def call(self, url, call_count=0) -> Optional[Dict]:
        if call_count >= 2:
            return None

        self._get_data_json = self.api_request(url=url)

        if self._get_data_json: # 응답값이 없으면 : 에러가 발생함
            return self._get_data_json
        else:
            time.sleep(10)
            return self.call(url, call_count + 1)

    def transform(self, data_key: str) -> None:
        self._pd_data = pd.DataFrame(self._get_data_json[data_key])
        self._pd_data['time'] = self._now
        print("Data Count : ", len(self._pd_data))

    def save(self, save_dir_path: str) -> str:
        file_path = f"{save_dir_path}{self._now.strftime('%Y-%m-%d-%H-%M-%S')}.parquet"
        self._pd_data.to_parquet(file_path, index=False)
        return file_path

    def transfer_s3(self, local_file_path: str, bucket: str, save_as_path: str) -> None:
        if self.__s3_client:
            raise KeyError(" aws_access_key_id / aws_secret_access_key Not Found")
        else:
            self.__s3_client.upload_file(local_file_path, bucket, save_as_path)


if __name__ == '__main__':


    key = os.environ["api_key"]
    aws_access_key_id = os.environ["aws_access_key_id"]
    aws_secret_access_key = os.environ["aws_secret_access_key"]

    while True:
        if dt.datetime.now().hour in [0, 1, 2, 3, 4]:
            time.sleep(60*10)
            continue
        position = CollectPublicData(aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key)
        arrival = CollectPublicData(aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key)

        _ = position.call(url=f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimePosition/0/100/1호선")
        _ = arrival.call(url=f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimeStationArrival/ALL")

        position.transform(data_key='realtimePositionList')
        position_file_path = position.save(save_dir_path="./data/position/")
        position.transfer_s3(position_file_path, bucket='subway_arrival', save_as_path=f'position/{position_file_path}')

        arrival.transform(data_key='realtimeArrivalList')
        arrival_file_path = arrival.save(save_dir_path="./data/arrival/")
        arrival.transfer_s3(arrival_file_path, bucket='subway_arrival', save_as_path=f'arrival/{arrival_file_path}')

        time.sleep(90)



