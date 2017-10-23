#!/usr/bin/python
import time
import json

import logging
import stomp
import config

logging.basicConfig()

S_STOPPED = 0
S_CONNECTING = 1
S_RUNNING = 2


conn = stomp.Connection(
    host_and_ports=[('datafeeds.networkrail.co.uk', 61618)],
    reconnect_attempts_max=60,
    heartbeats=(0, 0)
)

feeds = {
    'SIG': {
        'destination': '/topic/TD_ALL_SIG_AREA',
    },
    'MVT': {
        'destination': '/topic/TRAIN_MVT_ALL_TOC',
    },
}


class RFListener(stomp.ConnectionListener):
    def __init__(self, connection):
        self.state = S_STOPPED
        self.c = connection
        self.c.set_listener('', self)

    def on_connected(self, headers, message):
        print('connected: %s' % headers)
        for feed in feeds.keys():
            self.c.subscribe(
                destination=feeds[feed]['destination'],
                id=feed, ack='auto'
            )
        self.state = S_RUNNING

    def on_disconnected(self):
        print('disconnected')
        self.state = S_STOPPED
        try:
            self.c.stop()
        except Exception as e:
            print(e)

    def on_error(self, headers, message):
        print('error: %s' % message)

    def on_message(self, header, message):
        messages = json.loads(message)

        print(header)
        print(message)
        print("\t%d bytes" % len(message))

    def on_heartbeat_timeout(self, headers, message):
        print('timeout')

    def connect(self):
        self.state = S_CONNECTING
        self.c.start()
        self.c.connect(username=config.NR_USERNAME, passcode=config.NR_PASSWORD)

    def disconnect(self):
        self.state = S_STOPPED
        try:
            self.c.stop()
        except Exception as e:
            print(e)

rf = RFListener(conn)

try:
    while True:
        if rf.state == S_RUNNING:
            print('sleeping...')
            time.sleep(5)
        elif rf.state == S_CONNECTING:
            print('connecting...')
            time.sleep(5)
        elif rf.state == S_STOPPED:
            print('connect')
            rf.connect()
        else:
            raise Exception('unknown state');
except Exception as e:
    print(e)
    rf.disconnect()
