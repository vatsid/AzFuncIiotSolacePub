import logging

import azure.functions as func

import paho.mqtt.client as mqtt
import time
import random
import json

def publish(client, sensor, topic, qos, simulated_reading, trend):
    simulated_reading = simulated_reading + \
        trend * random.normalvariate(0.01, 0.005)
    payload = {"timestamp": int(time.time()), "device": sensor,
               "device_type": "plc",  "value": simulated_reading}
    jsonpayload_sensor = json.dumps(payload, indent=4)
    client.publish(topic, jsonpayload_sensor, qos=qos)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Connection parms for Solace Event broker
    solace_url = "mr1u6o37qngirf.messaging.solace.cloud"
    solace_port = 1883
    solace_user = "solace-cloud-client"
    solace_passwd = "m0k5fm1q3658gph4vm11gnnidl"
    solace_clientid = ""

    # Sensor Topics
    solace_topic_pressure = "devices/raspi-virtual/vats/987/mqtt/pressure/data"
    solace_topic_level = "devices/raspi-virtual/vats/987/mqtt/level/data"
    solace_topic_vibration = "devices/plc/wago/123/opc/vibration/data"

    # Instantiate/connect to mqtt client
    client = mqtt.Client(solace_clientid)
    client.username_pw_set(username=solace_user, password=solace_passwd)
    print("Connecting to solace {}:{} as {}".format(
        solace_url, solace_port, solace_user))
    client.connect(solace_url, port=solace_port)
    client.loop_start()

    #random.seed("temperature")  # A given device ID will always generate
    # the same random data

    simulated_nox = 50 + random.random() * 20
    simulated_sox = 40 + random.random() * 20
    simulated_pressure = 30 + random.random() * 20
    simulated_level = 15 + random.random() * 20
    simulated_vibration = 15 + random.random() * 20

    if random.random() > 0.5:
        sensor_trend = +1  # value will slowly rise
    else:
        sensor_trend = -1  # value will slowly fall

    # Publish num_messages mesages to the MQTT bridge once per second.
    num_messages = 100
    while num_messages > 0:

        publish(client, "level", solace_topic_level,
                1, simulated_level, sensor_trend)
        publish(client, "pressure", solace_topic_pressure,
                1, simulated_pressure, sensor_trend)
        #publish(client, "vibration", solace_topic_vibration, 1, simulated_vibration, sensor_trend)
        num_messages = num_messages-1
        time.sleep(2)
    client.loop_stop()
    client.disconnect()

    return {
        'statusCode': 200,
        'body': "Success"
    }
