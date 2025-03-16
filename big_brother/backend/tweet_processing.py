import logging
import time
from typing import Any, Dict
from confluent_kafka import Producer, Consumer
import pandas as pd

from utils import delivery_report
from big_brother.core import NerModel, ClassificationModel, BaseModel

logger = logging.getLogger("tweet_processing")


class TweetProcessorProducer:
    def __init__(
        self,
        db_connection_handler_host: str,
        db_connection_handler_port: str, 
        send_topic: str = "db_connection_handler_topic",
        sleep: bool = False
    ) -> None:
        self._conf = {
            'bootstrap.servers': f"{db_connection_handler_host}:{db_connection_handler_port}",
            'default.topic.config': {'api.version.request': True},
            'security.protocol': 'SSL'
        }
        self._db_connection_handler_host = db_connection_handler_host
        self._db_connection_handler_port = db_connection_handler_port
        self._send_topic = send_topic
        self._sleep = sleep

    def connect(self) -> None:
        logger.info(f"Connection to {self._db_connection_handler_host}:{self._db_connection_handler_port}")
        self._producer = Producer(self._conf)
        logger.info("Connection is established")

    def send(self, data: Any) -> None:
        self._producer.produce(self._send_topic, key='1', value=data, callback=delivery_report)
        self._producer.flush()
        logger.debug(f"Produced: {data}")
        if self._sleep:
            time.sleep(10)


class TweetProcessorConsumer:
    def __init__(
        self,
        tweet_processor_host: str,
        tweet_processor_port: str,
        read_topic: str = "raw_data_topic",
    ) -> None:
        self._conf = {
            'bootstrap.servers': f"{tweet_processor_host}:{tweet_processor_port}", 
            'group.id': 'dp_consumer',
        }
        self._tweet_processor_host = tweet_processor_host
        self._tweet_processor_port = tweet_processor_port
        self._read_topic = read_topic

        self._ner_model = NerModel()
        self._classification_model = ClassificationModel()
        self._sentiment_analysis_model = ClassificationModel()
            
    @property
    def producer(self) -> Any:
        _producer = TweetProcessorProducer(
            bootstrap_host="localhost",
            bootstrap_port="9096",
            send_topic="db_connection_handler_topic",
            sleep=True
        )
        _producer.connect()

        return _producer
    
    @property
    def models(self) -> Dict[str, BaseModel]:
        return {
            "ner": NerModel(),
            "classification": ClassificationModel(),
            "sentiment": ClassificationModel()
        }

    def connect(self) -> None:
        try:
            logger.info(f"Connection to {self._tweet_processor_host}:{self._tweet_processor_port}")
            self._consumer = Consumer(self._conf)
            self._consumer.subscribe([self._read_topic])
            logger.info("Connection is established")
        except BaseException:
            logger.exception("Connection to TweetProcessor is failed")
        
    def read(self) -> None:
        producer = self.producer
        logger.info(f"Read data from {self._read_topic}")
        tweet_id = 0
        while True:
            msg = self._consumer.poll(10)

            if msg is None:
                logger.info("Message is None")
                continue
            if msg.error():
                logger.info(f"Consumer error: {msg.error()}")
                continue

            logger.debug(f"Recieved message: {msg.value()}")
            
            df = pd.read_json(msg.value().decode('utf-8'), orient="index")
            df = df.transpose()
            logger.debug(f"Recived dataframe: {df}")
            
            data_to_send = {}
            for name, model in self.models:
                data_to_send[name] = model(df)

            data_to_send["tweet"] = df
            data_to_send["time"] = time.time()
            data_to_send["tweet_id"] = tweet_id

            data_to_send = pd.DataFrame(data_to_send)
            data_to_send = data_to_send.to_json()
            logger.debug(f"Preprocessed data: {data_to_send}")
            producer.send(data_to_send)

if __name__ == "__main__":
    consumer = TweetProcessorConsumer(
        tweet_processor_host="localhost",
        tweet_processor_port="9095",
        read_topic="raw_data_topic"
    )

    consumer.connect()
    consumer.read()