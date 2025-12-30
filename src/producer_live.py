from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

items = ["jakna", "majica", "tenisice", "hlace", "kapa"]
colors = ["crvena", "plava", "zelena", "crna", "bijela"]
actions = ["view", "click", "cart", "purchase"]

while True:
    event = {
        "item": random.choice(items),
        "boja": random.choice(colors),
        "akcija": random.choice(actions),
        "timestamp": int(time.time())
    }

    producer.send("live_events", event)
    print("Poslano:", event)
    time.sleep(1)   # jedna poruka svake sekunde
