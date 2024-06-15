import json
from confluent_kafka import Consumer, KafkaError

class RatingsConsumer:
    def __init__(self):
        self.config = {
            'bootstrap.servers': 'deciding-ray-10015-us1-kafka.upstash.io:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': 'ZGVjaWRpbmctcmF5LTEwMDE1JBFHdGiyJWAITeKX4WfE8A5BGKvlhrRHZmg1bmg',
            'sasl.password': 'YWU1ZDA3ODItNTZiZS00NGZiLWFiNzMtMTZiZmM5ZDE3YjA5',
            'group.id': 'rates_group',
            'auto.offset.reset': 'latest'
        }
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(['ratings'])

    def consume_messages(self):
        messages = []
        try:
            for _ in range(10):
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise Exception(msg.error().str())
                messages.append(json.loads(msg.value().decode('utf-8')))
        finally:
            print("Consume ratings done")
        return messages
