from confluent_kafka import Consumer, TopicPartition
from json import dumps
from time import sleep

value_serializer=lambda x: dumps(x).encode('utf-8')

topic = 'numtest3'

config = {
	'bootstrap.servers':'localhost',
	'group.id':'my-group2',
	'enable.auto.commit':True,
	'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
}
consumer = Consumer(config)
tp = TopicPartition(topic, 0)

consumer.subscribe([topic])


_, offset_max= consumer.get_watermark_offsets(tp)
offset_min = consumer.committed([tp])[0].offset

print(offset_min, offset_max)
sleep(2)

number = offset_max - min(offset_min, 0)

print(f"Debería leer un total de {number} mensajes")

messages = consumer.consume(num_messages = number, timeout=10)

if messages is None:
	raise ValueError('No he podido leer nada')


print("Hay un total de " + str(len(messages)) +" mensajes." )


for message in messages:
	print('Received message: {}, {}'.format(message.value().decode(), message.offset()))
	#consumer.commit(message)