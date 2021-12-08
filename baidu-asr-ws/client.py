import zmq
import decouple
import threading
import time
import json

try:
    context = zmq.Context()
    dealer = context.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, b'demo-client')

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


threading.Thread(target=heartbeart, args=(dealer,)).start()

th = threading.Thread(target=rcv, args=(dealer,))
th.start()
th.join()
