import logging
import time
from typing import Any, Dict
from confluent_kafka import Producer, Consumer
import pandas as pd
import json
from utils import delivery_report

import sys
sys.path.append('C:\\Studying\\BigBrother')
from backend.inference.models import NerModel, ClassificationModel, BaseModel

import os
import psutil
import onnxruntime
from transformers import AutoTokenizer

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
        with open('./configs/models_config.json', 'r') as config:
            self.models_config = json.load(config)
        self.max_batch_size = 8
        self.patience = 10
            
    @property
    def producer(self) -> Any:
        _producer = TweetProcessorProducer(
            db_connection_handler_host="localhost",
            db_connection_handler_port="9096",
            send_topic="db_connection_handler_topic",
            sleep=False
        )
        _producer.connect()

        return _producer

    def create_onnx_session(self, model_path):
        sess_options = onnxruntime.SessionOptions()
        sess_options.intra_op_num_threads = psutil.cpu_count(logical=True)

        session = onnxruntime.InferenceSession(model_path, sess_options,
                                               providers=["CUDAExecutionProvider", "CPUExecutionProvider"])
        return session
    
    @property
    def models(self) -> Dict[str, BaseModel]:
        ner_model_name = 'google-bert/bert-base-cased'

        tokenizer_kwargs = dict(trust_remote_code=True, clean_up_tokenization_spaces=False)
        tokenizer = AutoTokenizer.from_pretrained(ner_model_name, **tokenizer_kwargs)
        tokenizer.add_tokens(["{{URL}}", "{{USERNAME}}", "{@", "@}"])

        ner_model = self.create_onnx_session(self.models_config['ner']['path'])
        ner_model = NerModel(ner_model, tokenizer, self.models_config['ner']['id2label'])

        sent_model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
        tokenizer = AutoTokenizer.from_pretrained(sent_model_name, **tokenizer_kwargs)
        sent_model = self.create_onnx_session(self.models_config['sentiment']['path'])
        sent_model = ClassificationModel(sent_model, tokenizer, self.models_config['sentiment']['id2label'])

        model_name = "lucasresck/bert-base-cased-ag-news"
        class_model = self.create_onnx_session(self.models_config['category']['path'])
        tokenizer = AutoTokenizer.from_pretrained(model_name, **tokenizer_kwargs)
        class_model = ClassificationModel(class_model, tokenizer, self.models_config['category']['id2label'])

        return {
            "ner": ner_model,
            "category": class_model,
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
        messages = []
        last_infer = time.time()
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
            raw_tweet = df["tokens"][0]
            messages.append(raw_tweet)

            logger.debug(f"Raw tweet: {raw_tweet}")
            if len(messages) >= self.max_batch_size or (time.time() - last_infer) > self.patience:
                data_to_send = {}
                for name, model in models.items():
                    data_to_send[name] = model(messages)

                data_to_send["tweet"] = messages
                logger.debug(f"Data to send to database: {data_to_send}")
                data_to_send = json.dumps(data_to_send)
                producer.send(data_to_send)

                last_infer = time.time()
                messages = []


if __name__ == "__main__":
    consumer = TweetProcessorConsumer(
        tweet_processor_host="localhost",
        tweet_processor_port="9095",
    )

    consumer.connect()
    consumer.read()