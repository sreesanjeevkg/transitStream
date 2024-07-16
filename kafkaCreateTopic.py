import logging
from kafka.admin import NewTopic
from utils import kafkaUtils
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_topics_from_list(topic_list):
    kafka_utils = kafkaUtils()

    # Kafka broker configuration
    bootstrap_servers = "localhost:9092"
    client_id = 'my_kafka_admin_client'

    try:
        # Create Kafka admin client
        admin_client = kafka_utils.createKafkaAdmin(bootstrap_servers, client_id)

        # Convert the input list to NewTopic objects
        topics_to_create = [
            NewTopic(name=topic[0], num_partitions=topic[1], replication_factor=topic[2])for topic in topic_list
        ]

        # Create topics
        success = kafka_utils.createTopic(admin_client, topics_to_create)

        if success:
            logger.info("All topics created successfully")
        else:
            logger.warning("Some topics may not have been created")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Always close the admin client
        if 'admin_client' in locals():
            kafka_utils.closeKafkaAdmin(admin_client)

def main():
    if len(sys.argv) < 2:
        logger.error("Please provide the topic list as a command-line argument.")
        sys.exit(1)

    try:
        topicList = eval(sys.argv[1])
        if not isinstance(topicList, list):
            raise ValueError("Input must be a list")
        
        create_topics_from_list(topicList)
    except (SyntaxError, ValueError) as e:
        logger.error(f"Invalid input format: {e}")
        logger.info("Expected format: '[['topic_name', num_partitions, replication_factor], ...]'")
        sys.exit(1)

if __name__ == "__main__":
    main()