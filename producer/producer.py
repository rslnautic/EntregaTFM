from kafka import KafkaProducer
from kafka.errors import KafkaError
from time import sleep
from json import dumps
import os

filename = os.environ["STOCK_FILE_NAME"]
print(filename)
producer = KafkaProducer(bootstrap_servers=['kafka:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

with open("/data/"+filename, "r") as f:
	for x in f:
		split=x.split(",")
		data = {'stock': split[-1].replace("\n", ""), 'price': float(split[4]), 'date': split[0]}
		producer.send('test', key=split[-1].encode(), value=data)
		sleep(0.01)
