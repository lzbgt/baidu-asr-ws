import zmq
import decouple
import threading
import time
import json

context = zmq.Context()
dealer = context.socket(zmq.DEALER)
dealer.setsockopt(zmq.IDENTITY, b'demo-client')
try:

    addr = decouple.config('mq', 'tcp://127.0.0.1:2123')
    dealer.connect(addr)
    tra = dealer.send(b'hello')
    print(tra)
except:
    print('exception')
print(addr)


def heartbeart(dealer):
    while True:
        dealer.send(b'heartbeat')
        time.sleep(5)


def rcv(dealer):
    while True:
        msgs = dealer.recv_multipart()
        print(msgs[0].decode('utf-8'))


# threading.Thread(target=heartbeart, args=(dealer,)).start()

th = threading.Thread(target=rcv, args=(dealer,))
th.start()

while True:
    text = input("")  # Python 3
    if text == 'hello':
        dealer.send(b'hello')
    elif text == 'stop':
        dealer.send(b'stop')
    elif text == 'start':
        dealer.send(b'start')
    elif text == 'hb':
        dealer.send(b'heartbeat')
    elif text == 'exit':
        exit()
    else:
        print('unknonwn: ', text)
