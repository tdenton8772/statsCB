#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 18:15:51 2018

@author: tdenton
"""

import socket
import json
import re
from datetime import datetime
import time

class listener_definition(object):
    def __init__(self):
        self.UDP_IP = ""
        self.UDP_PORT = 0
        self.STATSD_HOSTNAME = ""
        self.STATSD_PORT = 0
        self.configure()
        self.types = {"c": "counter", "g":"gauge", "ms":"timer"}
        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    def configure(self):
       config_file = json.loads(open("./resources/config.json").read())
       self.UDP_IP = config_file['UDP_IP']
       self.UDP_PORT = config_file['UDP_PORT']
       self.STATSD_HOSTNAME = config_file['STATSD_HOSTNAME']
       self.STATSD_PORT = config_file['STATSD_PORT']
       
    def server(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((config.UDP_IP, config.UDP_PORT))
        
        while True:
            data, addr = self.sock.recvfrom(128*1024)
            self.send_sock.sendto(data, (self.STATSD_HOSTNAME, self.STATSD_PORT))
            json_data = processor(data)
            yield(json_data)
            
def processor(message):
    matches = re.match("^[\w.]+:[^|]+\|[^|]+(?:\|#(?:[\w.]+:[^,\n]+(?:,|$))*)?$", message)
    if matches:
        metric, unit = re.split("\|", re.split("\:+", message)[1])
        address = re.split("\.", re.split("\:+", message)[0])
        json_message = {}
        json_message['address'] = address
        json_message['level'] = len(address)
        json_message['metric'] = metric.strip()
        json_message['type'] = config.types[unit.strip()]
        json_message['time'] = int(time.mktime(datetime.timetuple(datetime.now()))*1000)
        json_message['comment'] = ""
        json_message['key'] = ".".join([(".".join(json_message['address'])), json_message['type'], str(json_message['time'])])
    else:
        json_message = {}
    return json_message


if __name__=="__main__":
    config = listener_definition()
    for data in config.server():
        print("Received: {}".format(data))
