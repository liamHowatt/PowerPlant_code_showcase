from db_credentials import db_credentials
import mysql.connector
import paho.mqtt.client as mqtt
from typing import Dict, Tuple, List
import time
import json
from nr_funcs import arr2float

class Tag:
    def __init__(self, id_, log_rate_ms):
        self.id_ = id_
        self.log_rate_ms = log_rate_ms
        self.log_rate_s = log_rate_ms / 1000
        self.last_log = float("-infinity")
    def check_and_update(self, _) -> bool:
        t = time.time()
        if self.last_log + self.log_rate_s > t:
            return False
        self.last_log = t
        return True

class DigitalTag:
    def __init__(self, id_, _):
        self.id_ = id_
        self.last_value = None
    def check_and_update(self, value) -> bool:
        if value == self.last_value:
            return False
        self.last_value = value
        return True


def on_connect(client, userdata, flags, rc):
    print("connected")
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT tag_id, tag_name, plc_id, log_rate_ms, logged
        FROM tags
        WHERE logged in (1, 2);
        """
    )
    for tag_id, tag_name, plc_id, log_rate_ms, logged in cursor:
        assert client.subscribe(f"nodered/plc/announce/{plc_id}/+/{tag_name}")[0] == 0
        assert logged in (1, 2)
        tags[(plc_id, tag_name)] = (Tag if logged==1 else DigitalTag)(tag_id, log_rate_ms)
    db.commit()
    print("subscribed to all")

def on_message(client, userdata, msg):
    if not msg.topic.startswith("nodered/plc/announce/"):
        return
    plc_id, _, tag_name = msg.topic.split("/")[-3:]
    plc_id = int(plc_id)
    tag = tags.get((plc_id, tag_name))
    if tag is None:
        return
    value = json.loads(msg.payload)
    if not tag.check_and_update(value):
        return
    if len(value) == 2:
        parsed_value = arr2float(value)
    else:
        parsed_value = int(value[0])
    buffer.append((parsed_value, tag.id_))
    if len(buffer) >= 1000:
        print("dumped!")
        cursor = db.cursor()
        cursor.executemany(
            "INSERT INTO log (value, tag_id) VALUES (%s, %d);",
            buffer
        )
        db.commit()
        buffer.clear()

buffer: List[Tuple[str, int]] = list()
db = mysql.connector.connect(**db_credentials)

tags: Dict[Tuple[int, str], Tag] = dict()

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect("localhost")

mqtt_client.loop_forever()
