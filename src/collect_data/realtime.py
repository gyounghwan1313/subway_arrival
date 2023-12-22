import os
import sys
from typing import Optional, Union, Dict, List
import time
import datetime as dt
import boto3
import traceback
import json

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
            api_id: Union[str, int],
            broker: Union[str, List[str]] = None,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
    ):
        """
        :param broker: : 데이터를 전송할 Broker 주소를 입력받음
        """

        self._now = dt.datetime.now()
        self._api_id = int(api_id)
        self.__sleep_time = None
        self._time_controller()

        self._get_data_json = None
        self._get_data = None
        self._pd_data = None


        self.__broker = broker
        self.__aws_access_key_id = aws_access_key_id
        self.__aws_secret_access_key = aws_secret_access_key

        if self.__aws_secret_access_key and self.__aws_secret_access_key:
            self.__aws_client = boto3.client(
                "firehose",
                aws_access_key_id=self.__aws_access_key_id,
                aws_secret_access_key=self.__aws_secret_access_key,
                region_name="ap-northeast-2"
            )
        else:
            self.__aws_client = None

    def _time_controller(self):

        if self._now.hour in [0, 1, 2, 3, 4]:
            logger.info(f"{self._now} : Invalid Collect Time")
            sys.exit(1)

        logger.info(f"API ID : {self._api_id}")
        if self._api_id == 1:
            if self._now.hour in [15, 16, 17, 18, 19, 20, 21, 22, 23]:
                logger.info(f"{self._now} : API ID [2] Collect Time")
                sys.exit(1)
            else:
                self.__sleep_time = 36

        elif self._api_id == 2 :
            if self._now.hour in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14]:
                logger.info(f"{self._now} : API ID [1] Collect Time")
                sys.exit(1)
            else:
                self.__sleep_time = 36
        else:
            logger.error("== ERROR ==")
            logger.error(f"== [{self.api_id}] API ID is Not Defined ==")

        if self.__sleep_time:
            logger.error("== ERROR ==")
            logger.error(f"== Sleep Time Not Set==")

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

    def call(self, url, call_count=0) -> Union[Dict, bool, None]:
        if call_count >= 2:
            return None

        self._get_data_json = self.api_request(url=url)

        # api 상태 코드가 200이지만, api 결과 코드가 정상이지 않을 때는 errorMessage기 없음
        # {'status': 500, 'code': 'INFO-200', 'message': '해당하는 데이터가 없습니다.', 'link': '', 'developerMessage': '', 'total': 0}

        if self._get_data_json:
            if "errorMessage" not in self._get_data_json:
                logger.info("==ERROR==")
                logger.info(f"{self._get_data_json}")
                if "code" not in self._get_data_json:
                    logger.info("==Code key is Not Found==")
                    logger.info(f"{self._get_data_json}")
                    sys.exit(1)
                    return None
                elif self._get_data_json["code"] == "ERROR-337":
                    logger.error("최대요청 건수 초과")
                    logger.error(f"{self._get_data_json}")
                    sys.exit(1)
                # 데이터 없음 에러 정상 처리
                elif self._get_data_json["code"] == "INFO-200":
                    return False
                else:
                    logger.error(f"{self._get_data_json}")
                    sys.exit(1)
                return None
            else:
                return self._get_data_json
        else:  # 응답값이 없으면 : 에러가 발생함 -> 2번 더 호출하고 실패하면 멈춤
            logger.info("Retry")
            logger.info(f"{call_count + 1}")
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
        [i.update({"time": int((self._now+dt.timedelta(hours=9)).timestamp())}) for i in self._json_data]

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

    def _producing_kafka(self, topic, unit: Optional[int] = None) -> None:
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
                    topic=topic, msg=self._json_data[start_idx: start_idx + unit]
                )
        else:
            kafka_conn.send_to_topic(topic=topic, msg=self._json_data)

    def producing_to_firehose(self, delivery_stream_name: str) -> None:
        for idx, data in enumerate(self._json_data):
            result = self.__aws_client.put_record(DeliveryStreamName=delivery_stream_name,
                                                  Record={'Data': json.dumps(data).encode('utf-8')})
            logger.info(f"[{idx + 1} / {len(self._json_data)}] result : {result}")

    def sleep(self):
        time.sleep(self.__sleep_time)

if __name__ == "__main__":
    api_id = os.environ["API_ID"]
    key = os.environ["api_key"]
    aws_access_key_id = os.environ['AWS_ACCESS_KEY']
    aws_secret_access_key = os.environ['AWS_SECRET_KEY']
    aws_firehose_stream_name = os.environ['AWS_FIREHOSE_STREAM']

    logger_class = LoadLogger()
    logger = logger_class.time_rotate_file(log_dir="/log/", file_name=f"realtime.log")
    logger.info(
        f"""==== ENV  Api Id : {api_id}, Key : {key} ===="""
    )

    api_call_count = 0

    try:
        while True:
            position = CollectPublicData(api_id=api_id,aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            # arrival = CollectPublicData(broker=[broker_1, broker_2, broker_3])

            logger.info("##### Position START #####")
            api_call_count += 1
            logger.info(f"===API CALL START [{api_call_count}]===")
            position_result = position.call(
                url=f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimePosition/0/100/1호선"
            )
            logger.info("===API CALL END===")
            if position_result:  # TRUE면
                logger.info("===TRANSFORM START===")
                position.transform(data_key="realtimePositionList")
                logger.info("===Firehose Connect Start===")
                position.producing_to_firehose(delivery_stream_name=aws_firehose_stream_name)
            else:
                logger.info("##### API Expected Error #####")
            logger.info("##### Position END #####")

            # logger.info("##### Arrival #####")
            # logger.info("===API CALL START===")
            # arrival_result = arrival.call(
            #     url=f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimeStationArrival/ALL"
            # )
            # logger.info("===API CALL END===")
            # if arrival_result:
            #     logger.info("===TRANSFORM START===")
            #     arrival.transform(data_key="realtimeArrivalList")
            #     logger.info("===Kafka Connect Start===")
            #     arrival.producing_kafka(topic=arrival_topic, unit=100)
            #
            # else:
            #     logger.info("##### API Expected Error #####")
            # logger.info("##### Arrival END #####")
            logger.info(f"### API CALL COUNT : {api_call_count} ####")
            logger.info("#################### DONE ####################")
            position.sleep()

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        logger.info(f"position._get_data_json : {position._get_data_json}")
        # logger.info(f"arrvial._get_data_json : {arrival._get_data_json}")
        sys.exit(1)
