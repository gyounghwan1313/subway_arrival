FROM python:3.10.13-slim-bullseye
LABEL authors="jayden"

RUN pip3 install requests pandas boto3

COPY src /src
RUN mkdir /log

WORKDIR /

CMD ["python3", "/src/collect_data/realtime.py"]