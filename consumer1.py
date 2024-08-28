from confluent_kafka import Consumer, KafkaError,KafkaException
import json
import os
import sys
import random
import requests
import cv2 

me = 'HossamFid-01'
topics=[me]
groupid=me+'group'

conf = {'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
        'group.id': groupid,
        'enable.auto.commit':True,
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)
consumer.subscribe(topics)

def msg_process(msg):
    choice = random.choice(['photo', 'car', 'person'])
    id = msg.value()
    id = msg.value().decode('utf-8')        
    response= requests.put('http://127.0.0.1:5000/object/'+id, json={"object": choice})

    if response.status_code == 200:
        print(f"Successfully updated ID: {id} with object: {choice}")
    else:
        print(f"Failed to update ID: {id} with object: {choice}")

running = True

def consumer_loop(consumer=consumer, topics=topics):
    try:
        consumer.subscribe(topics)
        while running:
 
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
    
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
           
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:

                msg_process(msg=msg)
    finally:

        consumer.close()

def shutdown():
    global running
    running = False
    
if __name__ == "__main__":
    consumer_loop()