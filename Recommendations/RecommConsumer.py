import json
from confluent_kafka import Consumer, KafkaError

class recommendationsConsumer:
    def __init__(self):
        self.config = {
            'bootstrap.servers': 'deciding-ray-10015-us1-kafka.upstash.io:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': 'ZGVjaWRpbmctcmF5LTEwMDE1JBFHdGiyJWAITeKX4WfE8A5BGKvlhrRHZmg1bmg',
            'sasl.password': 'YWU1ZDA3ODItNTZiZS00NGZiLWFiNzMtMTZiZmM5ZDE3YjA5',
            'group.id': 'predictions_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(['movies'])

    def consume_messages(self):
        consumer = self.consumer
        messages = []
        try:
            for _ in range(10):
                message = consumer.poll(1.0)  # Adjust polling timeout as needed
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        continue
                    elif message.error().code() != KafkaError._NO_ERROR:
                        print(f"Error: {message.error()}")
                        break
                else:
                    # Decode message and convert from JSON
                    decoded_message = json.loads(message.value().decode('utf-8'))
                    messages.append(decoded_message)
                    print(f"Received message: {decoded_message}")
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            consumer.close()
            print("Consumer closed")
        return messages
