import logging
from os import error
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
from kafka.admin import NewTopic
from kafka import KafkaProducer


class kafkaUtils:

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def createKafkaAdmin(self, bootstrap_servers: str, client_id: str) -> KafkaAdminClient:
        try:
            return KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id=client_id
            )
        except KafkaError as e:
            self.logger.error(f"Failed to create KafkaAdminClient: {e}")
            raise

    def createTopic(self, kafka_client: KafkaAdminClient, topics_list: list[NewTopic]):
        try:
            kafka_client.create_topics(new_topics=topics_list, validate_only=False)
            for topic in topics_list:
                self.logger.info(f"Topic '{topic.name}' created successfully")
            return True
        except KafkaError as e:
            self.logger.error(f"Failed to create topics: {e}")
            return False
    
    def closeKafkaAdmin(self, kafka_client: KafkaAdminClient):
        try:
            kafka_client.close()
            self.logger.info("KafkaAdminClient closed successfully")
        except KafkaError as e:
            self.logger.error(f"Error closing KafkaAdminClient: {e}")

    @staticmethod
    def createKafkaProducer( **kwargs) -> KafkaProducer:
        logger = logging.getLogger(__name__)
        try:
            return KafkaProducer(
                bootstrap_servers = kwargs['bootstrap_servers'],
                key_serializer = kwargs['key_serializer'],
                value_serializer = kwargs['value_serializer']
            )
        except KafkaError as e:
            logger.error("Failed to create producer")
            raise

    



