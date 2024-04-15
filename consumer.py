import pika
import json
from pymongo import MongoClient

mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['iot_data']
collection = db['mqtt_messages']

rabbitmq_params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(rabbitmq_params)
channel = connection.channel()

def process_message(channel, method, properties, body):
    try:
        message = json.loads(body)
        collection.insert_one(message)
        print(f"Message processed: {message}")

        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag)

channel.queue_declare(queue='mqtt_queue', durable=True)
channel.queue_bind(exchange='amq.topic', queue='mqtt_queue', routing_key='mytopic/#')
channel.basic_consume(queue='mqtt_queue', on_message_callback=process_message)
print('Waiting for messages...')
channel.start_consuming()

