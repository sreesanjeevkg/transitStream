from venv import logger
from utils import kafkaUtils
import json
import requests
import logging
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

def check_topic_exists(bootstrap_servers,client_id, topic_name):
    kafka_utils = kafkaUtils()
    admin_client = kafka_utils.createKafkaAdmin(bootstrap_servers, client_id)

    try:
        topics = admin_client.list_topics()
        return topic_name in topics
    finally:
        admin_client.close()

def apiRequest(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        raise
    
    except ValueError as e:
        print(f"Error decoding JSON response: {e}")
        raise

def preprocessJSON(message):
    val = []
    for key, value in message.items():
        if key == "entity":
            return value # since entity is a list - idk why it is a list

def sendMessages(producer, message, msg_key, topic_name = "hello"):
    try:
        # Check if the topic exists
        if not check_topic_exists(producer.config['bootstrap_servers'], producer.config['client_id'], topic_name):
            raise UnknownTopicOrPartitionError(f"Topic '{topic_name}' does not exist")
        
        # If the topic exists, send messages
        for value in message:
            producer.send(topic=topic_name, key=msg_key, value=value)
    
    except UnknownTopicOrPartitionError as e:
        # Re-raise the error if the topic doesn't exist
        raise e
    except Exception as e:
        # Handle any other unexpected errors
        raise Exception(f"An error occurred while sending messages: {str(e)}")
                


def main():
    
    logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    trip_updates_http = "http://transitdata.cityofmadison.com/TripUpdate/TripUpdates.json"
    vehicle_position_http = "http://transitdata.cityofmadison.com/Vehicle/VehiclePositions.json"
    
    producer = kafkaUtils.createKafkaProducer(bootstrap_servers='localhost:9092',
                                              key_serializer = str.encode,
                                              value_serializer = lambda msg: json.dumps(msg).encode('utf-8'))

    trip_updates = apiRequest(trip_updates_http)
    vehicle_position = apiRequest(vehicle_position_http)

    trip_updates_message = preprocessJSON(trip_updates)
    vehicle_position_message = preprocessJSON(vehicle_position)

    logger.info(f"Number of messages in trip updates is {len(trip_updates_message)}")

    sendMessages(producer, message = trip_updates_message, msg_key = "trip_updates")
    sendMessages(producer, message = vehicle_position_message, msg_key = "vehicle_position")

    producer.flush()

    producer.close()

if __name__ == "__main__":
    main()

