from multiprocessing import Value
from time import time
from flask import Flask, request, jsonify
from flask_caching import Cache
from flask_mqtt import Mqtt
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from nr_funcs import auto_encode
from db_credentials import uri
import json
import os
from math import inf

app = Flask(__name__)
CORS(app)

app.config["CACHE_TYPE"] = "MemcachedCache"
app.config["CACHE_MEMCACHED_SERVERS"] = ["127.0.0.1:11211"]
app.config["CACHE_KEY_PREFIX"] = ""
cache = Cache(app)

mqtt = Mqtt(app)
allowed_topics = [
    "nodered/plc/write/",
    "test/"
]

app.config['SQLALCHEMY_DATABASE_URI'] = uri
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

TOKEN_TIMEOUT = 60 * 60 * 24

@app.route("/")
def greeting():
    return "You're connected to the PowerPlant SCADA backend!"

@app.route("/restart")
def restart():
    os._exit(1)

@app.route("/mqtt-read", methods=["GET"])
def mqtt_read():
    topic = request.args.get("topic")
    if topic is None:
        return 'MQTT "topic" to read from not specified in URL params.', 400
    value = cache.get(topic)
    if value is not None:
        value = eval(value.decode())
    # might send you null of val not in cache
    return jsonify({"value": value})

@app.route("/mqtt-read-agg", methods=["POST"])
def mqtt_read_aggregate():
    json_in = request.json
    if not isinstance(json_in, list):
        return f"Expected array of topics to AND together (got a {type(json_in)})", 400
    vals = []
    for topic in json_in:
        value = cache.get(topic)
        if value is not None:
            value = eval(value.decode())
        vals.append(value)
    return jsonify({"value": all(vals)})

@app.route("/mqtt-write", methods=["POST"])
def mqtt_write():
    topic = request.args.get("topic")
    if topic is None:
        return 'MQTT "topic" to write to not specified in URL params.', 400
    if not any(topic.startswith(at) for at in allowed_topics):
        return f'Forbidden topic "{topic}"', 403
    value = request.args.get("value")
    if value is None:
        return '"value" to write not specified in URL params.', 400
    value = json.dumps(auto_encode(json.loads(value)))
    mqtt.publish(topic, value)
    return jsonify({})

@app.route("/historical-data", methods=["GET"])
def historical_data():
    tag_name = request.args.get("tag_name")
    if tag_name is None:
        return '"tag_name" to fetch historical data for is not specified in URL params.', 400
    result = db.session.execute(
        f"""
        SELECT l.value, l.timestamp
        FROM log l
        JOIN tags t ON t.tag_id=l.tag_id
        WHERE
            t.tag_name='{tag_name}' and
            timestamp > DATE_SUB(NOW(), INTERVAL 48 HOUR)
        ORDER BY l.timestamp ASC
        ;
        """
    )
    data = {"values": [], "timestamps": []}
    for row in result:
        data["values"].append(row["value"])
        data["timestamps"].append(str(row["timestamp"]))
    db.session.commit()
    return jsonify({"value": data})

@app.route("/warn_fault_get_limit", methods=["GET"])
def warn_fault_get_limit():
    num = int(request.args.get("num"))
    fault_cache = cache.get("//fault_cache")
    fault_cache = fault_cache if fault_cache is not None else b"[]"
    return jsonify(json.loads(fault_cache)[:num])

@app.route("/warn_fault_renew", methods=["GET"])
def warn_fault_renew():
    token = request.args.get("token")
    num = int(request.args.get("max"))

    fault_cache = cache.get("//fault_cache")
    fault_cache = json.loads(fault_cache if fault_cache is not None else b"[]")
    soonest = fault_cache[0]["timestamp"] if fault_cache else inf

    timestamp = cache.get("//tokens/" + token)
    if timestamp is not None:
        timestamp = float(timestamp)
        fault_cache = [fault for fault in fault_cache if fault["timestamp"] > timestamp]
    cache.set("//tokens/" + token, soonest, TOKEN_TIMEOUT)

    return jsonify(fault_cache[:num])
