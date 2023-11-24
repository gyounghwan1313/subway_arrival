
import sys
import traceback
import logging
from typing import Union, List, Any
import json
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

class KafkaConnector():

    def __init__(self, broker: Union[str, List[str]]):
        self.__broker = broker
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.__broker,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                acks=0,
                retries=3,
            )
        except Exception as e:
            logger.error(e)
            traceback.format_exc()
            sys.exit(1)

    def send_to_topic(self, topic: str, msg: Any) -> None:
        try:
            logger.info(f"Topic : {topic}")
            self._producer.send(topic, msg).get(timeout=5)
            logger.info("Send End")
        except Exception as e:
            logger.error(e)
            logger.error(f"Fail message : {msg}")
            traceback.format_exc()
            sys.exit(1)
