import logging

from confluent_kafka import Consumer

logger = logging.getLogger("db_connection_backend")
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console_formater = logging.Formatter("[ %(levelname)s ] %(message)s")
console.setFormatter(console_formater)
logger.addHandler(console)

class DBConnectionHandler:
    def __init__(
        self,
        db_connection_handler_host: str,
        db_connection_handler_port: str,
        read_topic: str = "db_connection_handler_topic",
    ) -> None:
        self._conf = {
            'bootstrap.servers': f"{db_connection_handler_host}:{db_connection_handler_port}", 
            'group.id': 'dp_consumer',
        }
        self._db_connection_handler_host = db_connection_handler_host
        self._db_connection_handler_port = db_connection_handler_port
        self._read_topic = read_topic

    def connect(self) -> None:
        try:
            logger.info(f"Connection to {self._db_connection_handler_host}:{self._db_connection_handler_port}")
            self._consumer = Consumer(self._conf)
            self._consumer.subscribe([self._read_topic])
            logger.info("Connection is established")
        except BaseException:
            logger.exception("Connection to TweetProcessor is failed")
        
    def read(self) -> None:
        logger.info(f"Read data from {self._read_topic}")
        while True:
            msg = self._consumer.poll(10)

            if msg is None:
                logger.info("Message is None")
                continue
            if msg.error():
                logger.info(f"Consumer error: {msg.error()}")
                continue

            logger.debug(f"Recieved message: {msg.value()}")

if __name__ == "__main__":
    consumer = DBConnectionHandler(
        db_connection_handler_host="localhost",
        db_connection_handler_port="9096",
    )

    consumer.connect()
    consumer.read()
        