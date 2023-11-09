
from src.module.api_status_check import api_status_check
import requests as req


@api_status_check
def api_call(url: str) -> object:
    return req.get(url=url)


if __name__ == '__main__':
    key = "insert key"
    data = api_call(f"http://swopenAPI.seoul.go.kr/api/subway/{key}/json/realtimePosition/0/100/1호선")
