from utils import kafkaUtils
from datetime import datetime
import time
import json
import requests
import logging

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

def sendMessages(producer, message, msg_key):
    for key, value in message.items():
        if key == "entity":
            for updates in value:
                producer.send(topic = "transitStream", key = msg_key, value = updates)
                


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

    sendMessages(producer, message = trip_updates, msg_key = "trip_updates")
    sendMessages(producer, message = vehicle_position, msg_key = "vehicle_position")

    producer.flush()

    producer.close()

if __name__ == "__main__":
    main()

