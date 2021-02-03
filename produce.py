from confluent_kafka import Producer
from json import dumps
from time import sleep

topic = 'numtest3'

producer = Producer({'bootstrap.servers': 'localhost:9092'})

value_serializer=lambda x: dumps(x).encode('utf-8')

for i in range(1,21):
	for j in range(1,11):
		producer.produce(topic, value_serializer({'number': j+(i*10)}))
		sleep(1)
	sleep(30)