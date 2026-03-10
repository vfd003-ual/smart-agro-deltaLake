import time
import json
import requests
from confluent_kafka import Producer

# Make sure your local Kafka broker is running on port 9092
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'iss-python-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Delivered to topic '{msg.topic()}' | Partition: [{msg.partition()}] | Key: {msg.key().decode('utf-8')}")

API_URL = "http://api.open-notify.org/iss-now.json"
KAFKA_TOPIC = "iss_telemetry"

print(f"🚀 Starting to pull ISS location and dumping to Kafka topic '{KAFKA_TOPIC}'...")

try:
    while True:
        try:
            response = requests.get(API_URL)
            
            if response.status_code == 200:
                data = response.json()
                timestamp = str(data['timestamp'])
                kafka_value = json.dumps(data).encode('utf-8')
                kafka_key = timestamp.encode('utf-8')
                
                producer.produce(
                    topic=KAFKA_TOPIC, 
                    key=kafka_key, 
                    value=kafka_value, 
                    callback=delivery_report
                )
                
                # Trigger any available delivery report callbacks
                producer.poll(0)
            else:
                print(f"Failed to fetch data: {response.status_code}")
        except requests.exceptions.ConnectTimeout:
            print("Connection timed out. Retrying...")
        except Exception as e:
            print(f"An error occurred: {e}")
            
        time.sleep(5)
        
except KeyboardInterrupt:
    print("\n🛑 Stopping producer...")

finally:
    print("Flushing remaining messages to Kafka...")
    producer.flush()
    print("Done!")
