
import traceback


def api_status_check(func):
    """

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
            else:
                # 200으로 떨어진 경우, response의 에러 코드확인
                result_json = result.json()
                error_message = result_json['errorMessage']

                print('errorMessage ', error_message)
                if error_message['status'] != 200:
                    return None
                if error_message['code'] != 'INFO-000':
                    return None

                return result_json

        except Exception as e:
            print(e)
            print(traceback.format_exc())

    return check