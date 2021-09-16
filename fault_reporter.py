import paho.mqtt.client as mqtt
import json
import nr_funcs
import time

import smtplib, ssl
from twilio.rest import Client

email_recipients = [
    "example@example.com" # Example
]

text_recipients = [
    "+15555555555", # Example
]

msg_body = """\
Fault on PLC 1

Fault tag name:
{name}

Time:
{time}
"""

TOPICS = [
    "nodered/plc/announce/1/fault/#",
    # "nodered/plc/announce/1/warning/#"
]
fault_states = {}

def on_connect(client, userdata, flags, rc):
    print("connected to MQTT")
    for topic in TOPICS:
        assert client.subscribe(topic)[0] == 0
    print("subscribed to")
    print(TOPIC)

def on_message(client, userdata, msg):
    global fault_states
    payload = nr_funcs.auto_decode(json.loads(msg.payload))
    assert type(payload) is bool
    old_state = fault_states.get(msg.topic, False)
    if payload and not old_state:
        report_fault(msg.topic.split("/")[-1])
    fault_states[msg.topic] = payload

def send_email(subject: str, body: str):
    email_content = f"Subject: {subject}\n\n{body}"
    with smtplib.SMTP_SSL("smtp.gmail.com", context=ssl.create_default_context()) as gmail:
        gmail.login(
            "fault reporter email address", # Example
            "password" # Example
        )
        for email_recipient in email_recipients:
            gmail.sendmail("fault reporter email address", email_recipient, email_content)

def send_texts(text_content: str):
    text_client = Client(
        "twilio sid", # Example
        "twilio auth token" # Example
    )
    for text_recipient in text_recipients:
        text_client.messages.create(
            to=text_recipient, 
            from_="+15555555", # Example (twilio phone number)
            body=text_content
        )

def report_fault(fault_name: str):

    t = time.ctime()
    msg = msg_body.format(name=fault_name, time=t)

    send_email(subject=fault_name, body=msg)

    send_texts(msg)

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect("localhost")

mqtt_client.loop_forever()
