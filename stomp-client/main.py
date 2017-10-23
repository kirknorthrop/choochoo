#!/usr/bin/python
import time
import json

import stomp
import config


class RailListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        print('error: %s' % message)

    def on_message(self, header, message):
        messages = json.loads(message)
        print(header)
        print("\t%d bytes" % len(message))

conn = stomp.Connection(host_and_ports=[('datafeeds.networkrail.co.uk', 61618)])
conn.set_listener('', RailListener())
conn.start()
conn.connect(config.NR_USERNAME, config.NR_PASSWORD, wait=True)
conn.subscribe(destination='/topic/TRAIN_MVT_ALL_TOC', id='MVT', ack='auto')

try:
    while True:
        time.sleep(5)
except Exception as e:
    print(e)
    conn.disconnect()
