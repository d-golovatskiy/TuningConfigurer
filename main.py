import json
from minio import Minio
from confluent_kafka import Consumer, KafkaException
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9096',
    'group.id': 'task-to-collect-result',
    'auto.offset.reset': 'earliest'
}

client = Minio(
    "localhost:9000",
    access_key="yPxRygfZmrQ2mVON1zgt",
    secret_key="agGwFd7jvL4TJMzMVSAcn5mWa3lKquLFlck9GLMi", secure=False
)

consumer = Consumer(config)
consumer.subscribe(['task-to-configure-queue-topic'])
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            print(f"Received message: {msg.value().decode('utf-8')}")
            parsed_message = json.loads(msg.value().decode('utf-8'))
            filenames = parsed_message['filenames']
            data = dict()
            for filename in filenames:
                try:
                    client.fget_object("collector", filename, filename)
                    with open("tmp/" + filename, "r") as file:
                        content = file.readlines()
                        data[','.join(content[0].replace('\n', '').split(',')[2:])] = content
                except:
                    print("Failed to download a file: " + filename)
            # process data here...
            print(data)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

