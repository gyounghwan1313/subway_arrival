
import os
import sys
from typing import Optional, Union, Dict, List
import time
import datetime as dt
import boto3
import traceback

import pandas as pd
import requests as req

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.module.logging_util import LoadLogger
from src.module.api_status_check import api_status_check
from src.module.kafka_connection import KafkaConnector

## API URL
# 도착 일괄 : http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimeStationArrival/ALL
# 실시간 위치 :"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimePosition/0/100/1호선


class CollectPublicData(object):

    """
    공공 데이터 수집 Class
    """

    def __init__(
        self,
        broker: Union[str, List[str]] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        """
        :param broker: : 데이터를 전송할 Broker 주소를 입력받음
        """

        self._get_data_json = None
        self._get_data = None
        self._pd_data = None
        self._now = None

        self.__broker = broker
        self.__aws_access_key_id = aws_access_key_id
        self.__aws_secret_access_key = aws_secret_access_key

        if aws_access_key_id and aws_secret_access_key:
            self.__s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.__aws_access_key_id,
                aws_secret_access_key=self.__aws_secret_access_key,
            )
        else:
            self.__s3_client = None

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

    def call(self, url, call_count=0) -> Union[Dict, bool]:
        if call_count >= 2:
            return None

        self._get_data_json = self.api_request(url=url)

        # api 상태 코드가 200이지만, api 결과 코드가 정상이지 않을 때는 errorMessage기 없음
        # {'status': 500, 'code': 'INFO-200', 'message': '해당하는 데이터가 없습니다.', 'link': '', 'developerMessage': '', 'total': 0}

        if self._get_data_json:
            if "errorMessage" not in self._get_data_json:
                logger.info("==ERROR==")
                if "code" not in self._get_data_json:
                    logger.info("==Code key is Not Found")
                    return None
                # 데이터 없음 에러 정상 처리
                elif self._get_data_json["code"] == "INFO-200":
                    return False
                return None
            else:
                return self._get_data_json
        else:  # 응답값이 없으면 : 에러가 발생함 -> 2번 더 호출하고 실패하면 멈춤
            logger.info("Retry")
            logger.info(f"{call_count+1}")
            time.sleep(10)
            return self.call(url, call_count + 1)

    def _transform_to_pdf(self, data_key: str) -> None:
        self._pd_data = pd.DataFrame(self._get_data_json[data_key])
        self._pd_data["time"] = self._now
        logger.info(f"Data Count : {len(self._pd_data)}")

    def transform(self, data_key: str) -> None:
        """
        응답 데이터 중 필요 없는 정보는 삭제하고 요청시간 key를 데이터 별로 삽입

        :param data_key: 필요한 정보가 들어 있는 Key
        :return:
        """
        self._json_data = self._get_data_json[data_key]
        logger.info(f"Data Count : {len(self._json_data)}")
        [i.update({"time": str(self._now)}) for i in self._json_data]

    def _save_pdf(self, save_dir_path: str) -> str:
        file_path = f"{save_dir_path}{self._now.strftime('%Y-%m-%d-%H-%M-%S')}.parquet"
        self._pd_data.to_parquet(file_path, index=False)
        logger.info(file_path)
        return file_path

    def _transfer_s3(
        self, local_file_path: str, bucket: str, save_as_path: str
    ) -> None:
        if self.__s3_client:
            self.__s3_client.upload_file(local_file_path, bucket, save_as_path)
        else:
            raise KeyError(" aws_access_key_id / aws_secret_access_key Not Found")

    # ToDo:  데이터값 유효성 검증 추가

    def producing_kafka(self, topic, unit: Optional[int] = None) -> None:
        """
        Kafka로 데이터 전송, 보낼 데이터가 많은 경우에는 unit 단위로 잘라서 보냄
        만약 보낼 데이터가 0개 이면 데이터를 보내지 않음

        :param topic: kafka의 topic 이름
        :param unit: 데이터를 분할해서 보낼 단위
        :return:
        """
        kafka_conn = KafkaConnector(broker=self.__broker)
        if unit:
            for start_idx in range(0, len(self._json_data), unit):
                kafka_conn.send_to_topic(
                    topic=topic, msg=self._json_data[start_idx : start_idx + unit]
                )
        else:
            kafka_conn.send_to_topic(topic=topic, msg=self._json_data)


if __name__ == "__main__":
    key = os.environ["api_key"]
    broker_1 = os.environ["KAFKA_BROKER_1_PRIVATE"]
    broker_2 = os.environ["KAFKA_BROKER_2_PRIVATE"]
    broker_3 = os.environ["KAFKA_BROKER_3_PRIVATE"]
    position_topic = os.environ["KAFKA_POSITION_TOPIC"]
    arrival_topic = os.environ["KAFKA_ARRIVAL_TOPIC"]

    logger_class = LoadLogger()
    logger = logger_class.time_rotate_file(log_dir="/log/", file_name=f"realtime.log")
    logger.info(
        f"""===========ENV===========\n Key : {key} \n Broker :{broker_1} / {broker_2} / {broker_3} \n Topics : {position_topic} / {arrival_topic} \n ========================="""
    )

    try:
        while True:
            if dt.datetime.now().hour in [0, 1, 2, 3, 4]:
                time.sleep(60)
                continue
            position = CollectPublicData(broker=[broker_1, broker_2, broker_3])
            arrival = CollectPublicData(broker=[broker_1, broker_2, broker_3])

            logger.info("##### Position START #####")
            logger.info("===API CALL START===")
            position_result = position.call(
                url=f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimePosition/0/100/1호선"
            )
            logger.info("===API CALL END===")
            if position_result:  # TRUE면
                logger.info("===TRANSFORM START===")
                position.transform(data_key="realtimePositionList")
                logger.info("===Kafka Connect Start===")
                position.producing_kafka(topic=position_topic, unit=None)
            else:
                logger.info("##### API Expected Error #####")
            logger.info("##### Position END #####")

            logger.info("##### Arrival #####")
            logger.info("===API CALL START===")
            arrival_result = arrival.call(
                url=f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimeStationArrival/ALL"
            )
            logger.info("===API CALL END===")
            if arrival_result:
                logger.info("===TRANSFORM START===")
                arrival.transform(data_key="realtimeArrivalList")
                logger.info("===Kafka Connect Start===")
                arrival.producing_kafka(topic=arrival_topic, unit=100)

            else:
                logger.info("##### API Expected Error #####")
            logger.info("##### Arrival END #####")
            logger.info("#################### DONE ####################")
            time.sleep(60)
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        logger.info(f"position._get_data_json : {position._get_data_json}")
        logger.info(f"arrvial._get_data_json : {arrival._get_data_json}")
        sys.exit(1)
