import sys
import traceback


def api_status_check(func):
    """
    API 상테 코드 및 결과 코드에 따라 결과값 리턴

    :param func: function
    :return:
    """

    def check(*args, **kwargs):
        try:
            result = func(*args, **kwargs)

            # API Status Code 체크
            print("API Status Code : ", result.status_code)
            if result.status_code != 200:
                return None
            # 200인 경우
            else:
                result_json = result.json()
                return result_json

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            # print(result_json)
            sys.exit(1)

    return check


