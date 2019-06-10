FROM python:3.7-alpine3.8

ENV AWS_DEFAULT_REGION=us-west-2

COPY ./ /opt/twitter-demo

WORKDIR /opt/twitter-demo

RUN pip install -r requirements.txt

CMD ["/opt/twitter-demo/src/stream_tweets.py"]

