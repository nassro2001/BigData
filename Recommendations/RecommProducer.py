from confluent_kafka import Producer
import json


class RecommendationsProducer:

    def __init__(self):
        # Kafka configuration
        self.config = {
            'bootstrap.servers': 'deciding-ray-10015-us1-kafka.upstash.io:9092',  # Replace with your server address
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': 'ZGVjaWRpbmctcmF5LTEwMDE1JBFHdGiyJWAITeKX4WfE8A5BGKvlhrRHZmg1bmg',  # Replace with your username
            'sasl.password': 'YWU1ZDA3ODItNTZiZS00NGZiLWFiNzMtMTZiZmM5ZDE3YjA5'
        }
        self.producer = Producer(self.config)

    def acked(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    def send_message(self, topic, value):
        try:
            value_data = json.loads(value)  # Convert JSON string to a dictionary
            movie_id_key = str(value_data['movieId'])  # Extract movieId and use it as the key
            print(value)
            self.producer.produce(topic, key=movie_id_key, value=value, callback=self.acked)
            self.producer.flush()
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
        except KeyError as e:
            print(f"Missing key in data: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
