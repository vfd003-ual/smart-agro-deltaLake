import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException

# 1. Configure the Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'iss-tracking-group',      # Consumer group ID
    'auto.offset.reset': 'earliest'        # Read from the beginning if no previous offset exists
}

consumer = Consumer(conf)

KAFKA_TOPIC = "iss_telemetry"

# 2. Subscribe to the topic
# Note: subscribe takes a list, because a consumer can listen to multiple topics
consumer.subscribe([KAFKA_TOPIC])

print(f"🎧 Listening to Kafka topic '{KAFKA_TOPIC}'...")
print("Waiting for messages (Press Ctrl+C to stop)\n" + "-"*50)

try:
    while True:
        # 3. Poll for new messages
        # The consumer checks for new data. If none is found within 1 second, it returns None.
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
            
        if msg.error():
            # Handle standard end-of-partition events
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            # Handle actual errors
            else:
                raise KafkaException(msg.error())

        # 4. Process the message
        # Decode the bytes back into a string, then parse the JSON
        raw_value = msg.value().decode('utf-8')
        data = json.loads(raw_value)
        
        # Extract the payload data
        lat = data['iss_position']['latitude']
        lon = data['iss_position']['longitude']
        unix_timestamp = data['timestamp']
        
        # Convert the Unix timestamp to a human-readable local time
        readable_time = datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        
        # Create a Google Maps link to the exact coordinates
        gmaps_url = f"https://www.google.com/maps/place/{lat},{lon}"

        # Print the formatted output
        print(f"🛰️  [ISS Update - {readable_time}]")
        print(f"   Latitude:  {lat}")
        print(f"   Longitude: {lon}")
        print(f"   Map Link:  {gmaps_url}\n")

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")

finally:
    # 5. Clean up
    # Closing the consumer ensures that your current read position (offset) is saved
    # and tells the Kafka broker that this consumer is leaving the group.
    consumer.close()
    print("Consumer closed gracefully.")