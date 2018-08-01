#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 18:15:51 2018

@author: tdenton
"""

import socket
import json
import re


class listener_definition(object):
    def __init__(self):
        self.UDP_IP = "127.0.0.1"
        self.UDP_PORT = 5005
        self.STATSD_HOSTNAME = "10.0.1.65"
        self.STATSD_PORT = 8125
        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    
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
        json_message['metric'] = metric.strip()
        json_message['type'] = unit.strip()
    else:
        json_message = {}
    return json_message


if __name__=="__main__":
    config = listener_definition()
    for data in config.server():
        print("Received: {}".format(data))
