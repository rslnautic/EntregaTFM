from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
from json import dumps, loads

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('test',
                         bootstrap_servers=['kafka:9092'], 
                         value_deserializer=lambda m: loads(m.decode('ascii')))
producer = KafkaProducer(bootstrap_servers=['kafka:9092'], 
							value_serializer=lambda m: dumps(m).encode('ascii'))

d = {}
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
    stock = message.value["stock"]
    price = message.value["price"]
    date = message.value["date"]

    if stock in d.keys():  
        d[stock].append(price)  
    else:  
        d[stock] = [price]
    
    #print("Stock: ", stock, ": ", d[stock])

    if len(d[stock]) >= 5:
    	data = {'stock': stock, 'prices': d[stock], 'date': date}
    	producer.send('test_agg', value=data)
    	d[stock].pop(0)
