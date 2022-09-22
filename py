PRODUCER CODE:
===================


from kafka import KafkaProducer

topic_name="cts_topic"
producer=KafkaProducer(bootstrap_servers=['localhost:9092'])

producer.send(topic_name,b'Welcome to Cognizant Technology Solutions - Data Engg Team!!!')
producer.flush()


CONSUMER CODE:
==============

from kafka import KafkaConsumer

topic_name='cts_topic'
consumer=KafkaConsumer(topic_name,
                      bootstrap_servers='localhost:9092',
                      auto_offset_reset='latest')
for message in consumer:
    print(message.value.decode('utf-8'))

