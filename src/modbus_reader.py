#!/usr/bin/env python3

# Reads data via Modbus (tcp/serial) from power meters
# Publishes Data to InfluxDB and MQTT

import os
import sys
import serial
import logging
import threading
import requests
from influxdb import InfluxDBClient
from datetime import datetime
from enum import Enum
import paho.mqtt.client as mqtt_client
from pymodbus.client import ModbusTcpClient
from pymodbus.client import ModbusSerialClient
import yaml
import numbers
import time

class ModbusReader(threading.Thread):
    def __init__(self, name, mqttc, influxc, bus, modbus_devices): 
        threading.Thread.__init__(self)
            
        self.name = name
        self.influxc = influxc
        self.mqttc = mqttc
        self.bus = bus
        self.client = None
        self.modbus_devices = modbus_devices

    def connect(self):
        logging.info(f"Connecting to bus: {self.name}")
        if self.bus['type'] == 'tcp':
            client = ModbusTcpClient(
                host=bus['host'],
                port=bus.setdefault('port', 502))
        elif self.bus['type'] == 'serial':
            client = ModbusSerialClient(
                port=bus['port'],
                baudrate=bus.setdefault('baudrate', 9600),
                bytesize=bus.setdefault('bytesize', 8),
                parity=bus.setdefault('parity', 'E'),
                stopbits=bus.setdefault('stopbits', 1))
        else:
            logging.fatal('Invalid bus type')
            exit(255)
        client.connect()
        self.client=client

    def run(self):
        start = False
        points = []
        while True:
            timestamp = datetime.now()
            self.bus.keys()
            for (unit_name, unit) in self.bus['units'].items():
                data = dict()
                for register in self.modbus_devices[unit['type']]:
                    if register['fetch']:
                        registers = self.client.read_holding_registers(register['address'], register['length'], unit['address']).registers
                        if (register['length'] == 1 and register['type'] == 'uint'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.UINT16)
                        elif (register['length'] == 2 and register['type'] == 'uint'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.UINT32)
                        elif (register['length'] == 4 and register['type'] == 'uint'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.UINT64)
                        elif (register['length'] == 1 and register['type'] == 'int'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.INT16)
                        elif (register['length'] == 2 and register['type'] == 'int'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.INT32)
                        elif (register['length'] == 4 and register['type'] == 'int'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.INT64)
                        elif (register['length'] == 2 and register['type'] == 'float'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.FLOAT32)
                        elif (register['length'] == 4 and register['type'] == 'float'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.FLOAT64)
                        elif (register['type'] == 'string'):
                            value = self.client.convert_from_registers(registers, self.client.DATATYPE.STRING)
                        else:
                            logging.error(f"Unknown data type: length {register['length']}, type {register['type']} in device type {unit['type']}, register {register['address']}")
                        
                        if isinstance(value, numbers.Number):
                            value = register['multiplicator'] * value
                        
                        data[register['name']] = value

                if self.influxc:
                    points += [{
                        "measurement": self.name + "." + unit_name,
                        "fields": data,
                        "time": timestamp.isoformat()
                    }]
                    try:
                        self.influxc.write_points(points)
                        points = []
                    except requests.exceptions.ConnectionError:
                        logging.error('Error sending data points! {} points cached.'.format(len(points)))

                if self.mqttc:
                    for key, value in data.items():
                        self.mqttc.publish(f"{mqtt_topic}/{self.name}/{unit_name}/{key}", value)

            runtime = (datetime.now()-timestamp).total_seconds()
            sleep_time = self.bus['interval'] - runtime
            if sleep_time < 0:
                logging.error(f'Runtime {runtime} is too long!')
            else:
                logging.info(f'Sleeping {sleep_time}s')
                time.sleep(sleep_time)

def mqtt_on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        logging.error(f"Failed to connect: {reason_code}. loop_forever() will retry connection")
    else:
        None

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    
    if os.getenv('MQTT_HOST') != None:
        mqtt_host = os.getenv('MQTT_HOST')
        mqtt_port = int(os.getenv('MQTT_PORT', '1883')) 
        mqtt_username = os.getenv('MQTT_USER')
        mqtt_password = os.getenv('MQTT_PASSWORD')
        mqtt_topic = os.getenv('MQTT_TOPIC', 'modbus_reader')

        mqttc = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        mqttc.on_connect = mqtt_on_connect
        mqttc.enable_logger()
        mqttc.loop_start()
        mqttc.username_pw_set(mqtt_username, mqtt_password)
        mqttc.connect(mqtt_host, mqtt_port, 60)
    else:
        mqttc = None
    
    if os.getenv('INFLUXDB_HOST') != None:
        influx_host = os.getenv('INFLUXDB_HOST')
        influx_port = int(os.getenv('INFLUXDB_PORT', '8086')) 
        influx_username = os.getenv('INFLUXDB_USER', 'meter_reader')
        influx_password = os.getenv('INFLUXDB_PASSWORD')
        influx_database = os.getenv('INFLUXDB_DATABASE', 'meter_reader')

        influxc = InfluxDBClient(influx_host, influx_port, influx_username, influx_password, influx_database)
    else:
        influxc = None
    
    threads = list()

    devices_dir = 'devices'
    devices_yaml = [f for f in os.listdir(devices_dir) if f.endswith('.yaml')]
    modbus_devices=dict()
    for f in devices_yaml:
        logging.info(f'Loading devices from file {f}')
        with open(devices_dir + '/' + f) as fd:
            modbus_devices.update(yaml.safe_load(fd))

    busses_dir = 'config'
    busses_yaml = [f for f in os.listdir(busses_dir) if f.endswith('.yaml')]
    modbus_busses=dict()
    for f in busses_yaml:
        logging.info(f'Loading busses from file {f}')
        with open(busses_dir + '/' + f) as fd:
            modbus_busses.update(yaml.safe_load(fd))

    for (name, bus) in modbus_busses.items():
        x = ModbusReader(name, mqttc, influxc, bus, modbus_devices)
        x.connect()
        threads.append(x)
        x.start()

    for index, thread in enumerate(threads):
        logging.info("Main    : before joining thread %d.", index)
        thread.join()
        logging.info("Main    : thread %d done", index)
