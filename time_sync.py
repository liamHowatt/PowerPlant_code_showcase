import paho.mqtt.publish as publish
import json
from nr_funcs import float2arr
import time

TIME_TAG = "time_UTC"
RATE_S = 1

def hour_float() -> float:
    t = time.gmtime()
    h = float(
        t.tm_hour + t.tm_min / 60 + t.tm_sec / 3600
    )
    return h

def main():
    while True:
        h = hour_float()
        publish.single(
            "nodered/plc/write/1/" + TIME_TAG,
            json.dumps(float2arr(h))
        )
        time.sleep(RATE_S)

if __name__ == "__main__":
    main()

