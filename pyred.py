from mysql.connector.fabric import connect
from pylibmodbus import ModbusTcp
from paho.mqtt.client import Client as MQTTClient, MQTT_ERR_SUCCESS
import json
from multiprocessing import Process, Event
from queue import Queue as LocalQueue
from typing import Tuple, List, Dict
import mysql.connector
from db_credentials import db_credentials
import time
import signal
from pygame.time import Clock


class MsgCallback:
    def __init__(self):
        self.que = LocalQueue()
    def handler(self, mqtt_cli, userdata, msg):
        self.que.put((msg.topic.split("/")[-1], msg.payload.decode()))


class CtrlC:
    def __init__(self):
        self.sig_received = False
    def handler(self, signal, frame):
        self.sig_received = True


def plc_loop_process(tt_str: str, plc_ip: str, plc_id: int, quit_event: Event):

    tt = json.loads(tt_str)
    assert isinstance(tt, list)
    
    if not tt: # empty
        quit_event.wait()
        return

    highest_bool, highest_hold = max_addrs(tt)
    tt_by_name = {t["tag_name"]: t for t in tt}

    modbus_cli = ModbusTcp(plc_ip)
    modbus_cli.connect()

    mqtt_cli = MQTTClient()
    msg_callback = MsgCallback()
    mqtt_cli.on_message = msg_callback.handler
    mqtt_cli.connect("localhost")
    assert mqtt_cli.subscribe(f"nodered/plc/write/{plc_id}/+", qos=0)[0] == MQTT_ERR_SUCCESS

    clock = Clock()
    while not quit_event.is_set():
        clock.tick(1)

        if highest_bool >= 0:
            bool_reg = modbus_cli.read_bits(0, highest_bool + 1)
        if highest_hold >= 0:
            hold_reg = modbus_cli.read_registers(0, highest_hold + 1)
        for tag in tt:
            if not tag["reported"]:
                continue

            if tag["data_type"] == "Bool":
                if bool_reg[tag["address"]]:
                    message_out = "[true,false,false,false,false,false,false,false]"
                else:
                    message_out = "[false,false,false,false,false,false,false,false]"
            elif tag["data_type"] == "Int":
                message_out = f'[{hold_reg[tag["address"]]}]'
            else: # Real
                message_out = f'[{hold_reg[tag["address"]]},{hold_reg[tag["address"] + 1]}]'

            if tag["tag_name"].upper().startswith("FAULT_"):
                severity = "fault"
            elif tag["tag_name"].upper().startswith("WARNING_"):
                severity = "warning"
            else:
                severity = "normal"

            assert mqtt_cli.publish(
                f'nodered/plc/announce/{plc_id}/{severity}/{tag["tag_name"]}',
                message_out,
                qos=0
            )[0] == MQTT_ERR_SUCCESS
        
        while not msg_callback.que.empty():
            tag_name, write_val = msg_callback.que.get()
            write_val = json.loads(write_val)
            tag = tt_by_name[tag_name]
            if tag["data_type"] == "Bool":
                bit = 1 if write_val[0] else 0
                modbus_cli.write_bit(tag["address"], bit)
            elif tag["data_type"] == "Int":
                modbus_cli.write_register(tag["address"], write_val[0])
            else: # Real
                modbus_cli.write_registers(tag["address"], write_val)

        mqtt_cli.loop()

    modbus_cli.close()
    mqtt_cli.disconnect()


def max_addrs(tt: List[dict]) -> Tuple[int, int]:
    highest_bool = -1
    highest_hold = -1
    for tag in tt:
        if tag["data_type"] == "Bool":
            if tag["address"] > highest_bool:
                highest_bool = tag["address"]
        else: # Int or Real
            if tag["address"] > highest_hold:
                highest_hold = tag["address"]
                if tag["data_type"] == "Real":
                    highest_hold += 1
    return highest_bool, highest_hold


def query_db() -> Dict[int, str]:
    db = mysql.connector.connect(**db_credentials)
    cur = db.cursor()
    cur.execute(
        """
        SELECT ip_address, node_red_id
        FROM powerplant_exp_unit_2.plc
        ;
        """
    )
    id_ip = {id_: ip for ip, id_ in cur}
    db.disconnect()
    return id_ip

# {
#     "tag_name": "time_UTC",
#     "fc": {
#         "read": 3,
#         "write": 16
#     },
#     "address": 60,
#     "quantity": 2,
#     "data_type": "Real"
# }

def main():

    mqtt_cli = MQTTClient()
    msg_callback = MsgCallback()
    mqtt_cli.on_message = msg_callback.handler
    mqtt_cli.connect("localhost")
    assert mqtt_cli.subscribe("nodered/plc/tag_table/+", qos=0)[0] == MQTT_ERR_SUCCESS

    ctrl_c = CtrlC()
    signal.signal(signal.SIGINT, ctrl_c.handler)
    processes: Dict[int, Tuple[Process, Event]] = dict()
    tt_backup: Dict[int, str] = dict()

    while not ctrl_c.sig_received:

        if not msg_callback.que.empty():
            id_ip = query_db()
        while not msg_callback.que.empty():
            id_, tt_str = msg_callback.que.get()
            id_ = int(id_)
            tt_backup[id_] = tt_str
            p_e = processes.get(id_)
            if p_e is not None:
                p, e = p_e
                e.set()
                p.join(timeout=1)
                if p.is_alive():
                    print("had to commit murder...")
                    p.terminate()
            e = Event()
            p = Process(
                target=plc_loop_process,
                args=(tt_str, id_ip[id_], id_, e),
                daemon=True
            )
            p.start()
            processes[id_] = (p, e)

        died = []
        for id_, (p, e) in processes.items():
            if not p.is_alive():
                print(f"PLC {id_} died for some reason")
                died.append(id_)
        if died:
            id_ip = query_db()
        for id_ in died:
            e = Event()
            p = Process(
                target=plc_loop_process,
                args=(tt_backup[id_], id_ip[id_], id_, e),
                daemon=True
            )
            p.start()
            processes[id_] = (p, e)

        mqtt_cli.loop()

        time.sleep(0.25)

    print(" ctrl-c")

    for p, e in processes.values():
        e.set()

    mqtt_cli.disconnect()

    t = time.time()
    for p, e in processes.values():
        p.join(timeout=1)
        if time.time() - t >= 1:
            print("outta time")
            break


if __name__ == "__main__":
    main()
