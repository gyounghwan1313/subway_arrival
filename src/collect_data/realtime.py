
import os
import sys
from typing import Optional, Union, Dict, List
import time
import datetime as dt
import boto3

import pandas as pd
import requests as req

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.module.logging_util import LoadLogger
from src.module.api_status_check import api_status_check
from src.module.kafka_connection import KafkaConnector


logger_class = LoadLogger()
logger = logger_class.time_rotate_file(log_dir="/log/", file_name=f"{__name__}")

## API URL
# 도착 일괄 : http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimeStationArrival/ALL
# 실시간 위치 :"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimePosition/0/100/1호선


class CollectPublicData(object):

    def __init__(self,
                 broker: Union[str, List[str]] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None):

        self._get_data = None
        self._pd_data = None
        self._now = None

        self.__broker = broker
        self.__aws_access_key_id = aws_access_key_id
        self.__aws_secret_access_key = aws_secret_access_key

        if aws_access_key_id and aws_secret_access_key:
            self.__s3_client = boto3.client("s3",
                                            aws_access_key_id=self.__aws_access_key_id,
                                            aws_secret_access_key=self.__aws_secret_access_key)
        else:
            self.__s3_client = None

    @api_status_check
    def api_request(self, url: str) -> Optional[req.Response]:
        self._now = dt.datetime.now()
        logger.info(f" time : {self._now}")
        self._result = req.get(url=url)

        return self._result

    def call(self, url, call_count=0) -> Optional[Dict]:
        if call_count >= 2:
            return None

        self._get_data_json = self.api_request(url=url)

        # api 상태 코드가 200이지만, api 결과 코드가 정상이지 않을 때는 errorMessage기 없음
        # {'status': 500, 'code': 'INFO-200', 'message': '해당하는 데이터가 없습니다.', 'link': '', 'developerMessage': '', 'total': 0}

        if self._get_data_json:
            if 'errorMessage' not in self._get_data_json:
                logger.info("==ERROR==")
                if "code" not in self._get_data_json:
                    logger.info("==Code key is Not Found")
                    return None
                return None
            else:
                return self._get_data_json
        else: # 응답값이 없으면 : 에러가 발생함 -> 2번 더 호출하고 실패하면 멈춤
            logger.info("Retry")
            logger.info(f"{call_count+1}")
            time.sleep(10)
            return self.call(url, call_count + 1)

    def _transform_to_pdf(self, data_key: str) -> None:
        self._pd_data = pd.DataFrame(self._get_data_json[data_key])
        self._pd_data['time'] = self._now
        logger.info(f"Data Count : {len(self._pd_data)}")

    def transform(self, data_key: str) -> None:
        self._json_data = self._get_data_json[data_key]
        [i.update({"time": self._now}) for i in self._json_data]
        logger.info(f"Data Count : {len(self._json_data)}")
        if len(self._json_data) == 0:
            sys.exit(1)

    def _save_pdf(self, save_dir_path: str) -> str:
        file_path = f"{save_dir_path}{self._now.strftime('%Y-%m-%d-%H-%M-%S')}.parquet"
        self._pd_data.to_parquet(file_path, index=False)
        logger.info(file_path)
        return file_path

    def _transfer_s3(self, local_file_path: str, bucket: str, save_as_path: str) -> None:
        if self.__s3_client:
            self.__s3_client.upload_file(local_file_path, bucket, save_as_path)
        else:
            raise KeyError(" aws_access_key_id / aws_secret_access_key Not Found")

    # ToDo:  데이터값 유효성 검증 추가

    def producing_kafka(self, topic):
        kafka_conn = KafkaConnector(broker=self.__broker)
        kafka_conn.send_to_topic(topic=topic, msg=self._json_data)


if __name__ == '__main__':

    key = os.environ["api_key"]
    broker_1 = os.environ["KAFKA_BROKER_1_PRIVATE"]
    broker_2 = os.environ["KAFKA_BROKER_2_PRIVATE"]
    broker_3 = os.environ["KAFKA_BROKER_3_PRIVATE"]
    position_topic = os.environ['KAFKA_POSITION_TOPIC']
    arrival_topic = os.environ['KAFKA_ARRIVAL_TOPIC']

    logger.info(f"""===========ENV===========
                    Key : {key} \n 
                    Broker :{broker_1} / {broker_2} / {broker_3} \n 
                    Topics : {position_topic} / {arrival_topic} \n
                    =========================""")

    while True:
        if dt.datetime.now().hour in [0, 1, 2, 3, 4]:
            time.sleep(60)
            continue
        position = CollectPublicData(broker=[broker_1, broker_2, broker_3])
        arrival = CollectPublicData(broker=[broker_1, broker_2, broker_3])

        _ = position.call(url=f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimePosition/0/100/1호선")
        _ = arrival.call(url=f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimeStationArrival/ALL")

        position.transform(data_key='realtimePositionList')
        position.producing_kafka(topic=position_topic)

        arrival.transform(data_key='realtimeArrivalList')
        arrival.producing_kafka(topic=arrival_topic)

        time.sleep(60)
