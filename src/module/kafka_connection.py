
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
            logger.info(e)
            traceback.format_exc()
            sys.exit(1)

    def send_to_topic(self, topic: str, msg: Any) -> None:
        try:
            self._producer.send(topic, msg).get(timeout=5)
        except Exception as e:
            logger.info(e)
            traceback.format_exc()
            sys.exit(1)
