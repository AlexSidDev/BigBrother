import logging
import time
from typing import Any, Dict
from confluent_kafka import Producer, Consumer
import pandas as pd

from utils import delivery_report
from backend.inference.models import NerModel, ClassificationModel, BaseModel

from backend.model_utils import create_model
from transformers import AutoTokenizer

from transformers import AutoTokenizer, AutoModelForSequenceClassification
from backend.inference.models import ClassificationModel

logger = logging.getLogger("tweet_processing")
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console_formater = logging.Formatter("[ %(levelname)s ] %(message)s")
console.setFormatter(console_formater)
logger.addHandler(console)

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
            
    @property
    def producer(self) -> Any:
        _producer = TweetProcessorProducer(
            db_connection_handler_host="localhost",
            db_connection_handler_port="9093",
            send_topic="db_connection_handler_topic",
            sleep=False
        )
        _producer.connect()

        return _producer
    
    @property
    def models(self) -> Dict[str, BaseModel]:
        label_2_id = {
                 "B-corporation": 0,
                 "B-creative_work": 1,
                 "B-event": 2,
                 "B-group": 3,
                 "B-location": 4,
                 "B-person": 5,
                 "B-product": 6,
                 "I-corporation": 7,
                 "I-creative_work": 8,
                 "I-event": 9,
                 "I-group": 10,
                 "I-location": 11,
                 "I-person": 12,
                 "I-product": 13,
                 "O": 14
             }
        model_name = 'google-bert/bert-base-cased'                              

        model = create_model(model_name, label_2_id)
        tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True, clean_up_tokenization_spaces=False)
        tokenizer.add_tokens(["{{URL}}", "{{USERNAME}}", "{@", "@}"])
        model.resize_token_embeddings(len(tokenizer))
        ner_model = NerModel(model, tokenizer, {v: k for k, v in label_2_id.items()}, device='cuda')


        model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
        model = AutoModelForSequenceClassification.from_pretrained(model_name)
        tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True, clean_up_tokenization_spaces=False)
        class_model = ClassificationModel(model, tokenizer, model.config.id2label, device='cuda')

        model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
        model = AutoModelForSequenceClassification.from_pretrained(model_name)
        tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True, clean_up_tokenization_spaces=False)
        sent_model = ClassificationModel(model, tokenizer, model.config.id2label, device='cuda')

        return {
            "ner": ner_model,
            "classification": class_model,
            "sentiment": sent_model
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
        models = self.models
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
            # TODO (@a.klykov) Нужно достать из датафрейма твит и отдать моделям или можно просто msg.values() отдать?
            data_to_send = {}
            for name, model in models:
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
        tweet_processor_port="9092",
    )

    consumer.connect()
    consumer.read()