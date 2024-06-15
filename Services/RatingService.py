from confluent_kafka import Producer
import json


class RatingsProducer:
    def __init__(self):
        # Kafka configuration
        self.config = {
            'bootstrap.servers': 'deciding-ray-10015-us1-kafka.upstash.io:9092',  # Replace with your server address
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': 'ZGVjaWRpbmctcmF5LTEwMDE1JBFHdGiyJWAITeKX4WfE8A5BGKvlhrRHZmg1bmg',  # Replace with your username
            'sasl.password': 'YWU1ZDA3ODItNTZiZS00NGZiLWFiNzMtMTZiZmM5ZDE3YjA5'   # Replace with your password
        }
        self.producer = Producer(self.config)

    def acked(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    def send_message(self, topic, key, value):
        message = json.dumps(value)
        self.producer.produce(topic, key=str(key), value=message, callback=self.acked)
        self.producer.flush()
