import time
import json
from confluent_kafka import Producer
import requests 

BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'coinbase'

# Replace 'YOUR_API_KEY' with your actual API key
api_key = '7e7f6cb4-67f1-46f2-b80e-2af865a8ced7'
url = 'https://api.coincap.io/v2/markets'
headers = {
    'Authorization': f'Bearer {api_key}'
}

try:
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    while True:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            markets = data['data']

            for market in markets:
                market_data = json.dumps(market).encode('utf-8')
                producer.produce(KAFKA_TOPIC, key=None, value=market_data)
                producer.flush()
                print("Successfully sent a message to Kafka.")


        else:
            print(f"Error: {response.status_code} - {response.text}")

        time.sleep(10)

except KeyboardInterrupt:
    print("Program terminated by user.")
except Exception as e:
    print(f"An error occurred: {e}")

























