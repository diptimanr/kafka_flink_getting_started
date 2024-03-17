import json
import random
from config import config

from confluent_kafka import Producer
import time

def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Device reading for {event.key().decode("utf8")} produced to {event.topic()}')
def sensor_event():

    DEVICES = ['b8:27:eb:bf:9d:51', '00:0f:00:70:91:0a', '1c:bf:ce:15:ec:4d']
    device_id = random.choice(DEVICES)
    co = round(random.uniform(0.0011, 0.0072), 4)
    humidity = round(random.uniform(45.00, 78.00), 2)
    motion = random.choice([True, False])
    temp = round(random.uniform(17.00, 36.00), 2)
    amp_hr = round(random.uniform(0.10, 1.80), 2)
    event_ts = int(time.time() * 1000)

    sensor_event = {
        'device_id': device_id,
        'co': co,
        'humidity': humidity,
        'motion': motion,
        'temp': temp,
        'ampere_hour': amp_hr,
        'ts': event_ts
    }
    return sensor_event

if __name__ == '__main__':
    topic = 'sensor.readings'
    device_data = sensor_event()
    producer = Producer(config)

    try:
        while True:
            device_data = sensor_event()
            print(json.dumps(device_data))
            producer.produce(topic=topic, key=device_data['device_id'],
                             value=json.dumps(device_data),
                             on_delivery=delivery_report)
            time.sleep(5)
    except Exception as e:
        print(e)
    finally:
        producer.flush()