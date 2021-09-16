import pygame
import paho.mqtt.client as mqtt
import uuid
import io
import time
from dataclasses import dataclass

pic_size = (640, 480)
windowSize = (pic_size[0] * 2, pic_size[1])
FRAME = pygame.Surface(pic_size)
FRAME.fill((0, 0, 0))

@dataclass
class UserData:
    frame1: pygame.Surface = FRAME
    frame2: pygame.Surface = FRAME
    ping1: float = float("-infinity")
    ping2: float = float("-infinity")
    uuid: str = ""

def on_message(mqtt_cli, userdata, msg):
    split = msg.topic.split("/")
    if split[3] == "ping":
        if split[2] == "1":
            userdata.ping1 = time.time()
        elif split[2] == "2":
            userdata.ping2 = time.time()
        else:
            raise Exception("impossible ping cam id")
    elif split[3] == "pic-reply":
        uuid_ = split[4]
        if uuid_ == userdata.uuid:
            new_frame = pygame.image.load(io.BytesIO(msg.payload), "image.jpg")
            assert new_frame.get_width() == pic_size[0] and new_frame.get_height() == pic_size[1]
            if split[2] == "1":
                userdata.frame1 = new_frame
            elif split[2] == "2":
                userdata.frame2 = new_frame
            else:
                raise Exception("impossible pic-reply cam id")
            if userdata.frame1 is not FRAME and userdata.frame2 is not FRAME:
                pygame.image.save(userdata.frame1, f"C:\\Users\\liamj\\Documents\\stereocam\\output\\{uuid_}_1.jpg")
                pygame.image.save(userdata.frame2, f"C:\\Users\\liamj\\Documents\\stereocam\\output\\{uuid_}_2.jpg")
    else:
        raise Exception("impossible message")

def on_connect(mqtt_cli, userdata, flags, rc):
    assert mqtt_cli.subscribe("picam/1/+/pic-reply/+")[0] == 0
    assert mqtt_cli.subscribe("picam/1/+/ping")[0] == 0


userdata = UserData()
assert userdata.frame1 is FRAME
mqtt_cli = mqtt.Client(userdata=userdata)
mqtt_cli.on_message = on_message
mqtt_cli.on_connect = on_connect
mqtt_cli.connect("localhost")

do_cap = False

pygame.init()
window = pygame.display.set_mode(windowSize)
background = pygame.Surface(windowSize)

clock = pygame.time.Clock()
while True:
    clock.tick(30)
    mqtt_cli.loop(timeout=0.03)

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            exit()
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_SPACE:
                do_cap = True
    
    if mqtt_cli.is_connected() and do_cap:
        do_cap = False
        userdata.frame1 = FRAME
        userdata.frame2 = FRAME
        new_uuid = str(uuid.uuid1())
        userdata.uuid = new_uuid
        assert mqtt_cli.publish(
            "picam/1/all/take-pic",
            new_uuid,
            qos=0, retain=False
        )[0] == 0
    
    background.blit(userdata.frame2, (pic_size[0], 0))
    pygame.draw.circle(
        background,
        (0, 255, 0) if (time.time() - userdata.ping2) < 3 else (255, 0, 0),
        (pic_size[0], 0),
        10
    )

    background.blit(userdata.frame1, (0, 0))
    pygame.draw.circle(
        background,
        (0, 255, 0) if (time.time() - userdata.ping1) < 3 else (255, 0, 0),
        (0, 0),
        10
    )

    window.blit(background, (0, 0))
    pygame.display.flip()

