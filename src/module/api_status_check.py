
from collections import Callable
from typing import Optional, Dict, Any
import sys
import traceback
import logging

logger = logging.getLogger(__name__)


def api_status_check(func: Callable[str]) -> Callable:
    """
    API 상테 코드 및 결과 코드에 따라 결과값 리턴

    :param func: function
    :return:
    """

    def check(*args, **kwargs) -> Optional[Dict[Any]]:
        try:
            result = func(*args, **kwargs)

            # API Status Code 체크
            logger.info(f"API Status Code : {result.status_code}", )
            if result.status_code != 200:
                return None
            # 200인 경우
            else:
                result_json = result.json()
                return result_json

        except Exception as e:
            logger.info(e)
            logger.info(traceback.format_exc())
            sys.exit(1)

    return check


