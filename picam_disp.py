import pygame
import paho.mqtt.client as mqtt
import uuid
import io
import time

windowSize = (640, 480)

class MsgCallback:
    def __init__(self, storage):
        self.storage = storage
        self.waiting_for = None
        self.last_ping = float("-infinity")
    def callback(self, mqtt_cli, userdata, msg):
        print(msg.topic)
        if msg.topic.startswith("picam/1/1/pic-reply/"):
            if msg.topic.split("/")[-1] == self.waiting_for:
                self.storage = pygame.image.load(
                    io.BytesIO(msg.payload),
                    "image.jpg"
                )
                assert self.storage.get_width() == windowSize[0]
                assert self.storage.get_height() == windowSize[1]
                self.waiting_for = None
        elif msg.topic == "picam/1/1/ping":
            self.last_ping = time.time()
        else:
            raise Exception("impossible message")

def on_connect(mqtt_cli, userdata, flags, rc):
    assert mqtt_cli.subscribe("picam/1/1/pic-reply/+")[0] == 0
    assert mqtt_cli.subscribe("picam/1/1/ping")[0] == 0

pygame.init()
window = pygame.display.set_mode(windowSize)
frame = pygame.Surface(windowSize)
frame.fill((0, 0, 0,))

mqtt_cli = mqtt.Client()
msg_callback = MsgCallback(frame)
mqtt_cli.on_message = msg_callback.callback
mqtt_cli.on_connect = on_connect
mqtt_cli.connect("localhost")

sent_at = float("-infinity")

clock = pygame.time.Clock()
while True:
    clock.tick(30)
    mqtt_cli.loop()

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            exit()
    
    if mqtt_cli.is_connected() and (msg_callback.waiting_for is None or (time.time() - sent_at) > 5):
        new_uuid = str(uuid.uuid1())
        msg_callback.waiting_for = new_uuid
        assert mqtt_cli.publish(
            "picam/1/all/take-pic",
            new_uuid,
            qos=0, retain=False
        )[0] == 0
        sent_at = time.time()
    
    pygame.draw.circle(
        msg_callback.storage,
        (0, 255, 0) if (time.time() - msg_callback.last_ping) < 3 else (255, 0, 0),
        (0, 0),
        10
    )
    
    window.blit(msg_callback.storage, (0, 0))
    pygame.display.flip()

