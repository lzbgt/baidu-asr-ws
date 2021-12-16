import websocket
import threading
import time
import json
import pyaudio
import zmq
import decouple
import queue

url = decouple.config(
    'api', 'wss://vop.baidu.com/realtime_asr?sn=e9046d79-ec33-443a-adc0-cdc5791764d8')

# hint: swapid
start_data = {
    "type": "START",
    "data": {
        "appid": int(decouple.config('appid', 25299031)),
        "appkey": decouple.config('appkey', 'DSWFRHTyE0S0BbYfZYimU6wu'),
        "dev_pid": decouple.config('devid',  15372),
        "cuid": decouple.config('cuid', 'cuid1'),
        "format": "pcm",
        "sample": 16000
    }
}

qRcv = queue.Queue()
qSend = queue.Queue()
mClients = {}


class MQ(object):
    router = None
    started = False
    HEARTBEAT = 60*5

    def __init__(self):
        if not self.router:
            self.context = zmq.Context()
            self.router = self.context.socket(zmq.ROUTER)
            self.router.bind(decouple.config('mq', 'tcp://0.0.0.0:2123'))

    def __call__(self, *args):
        def rcv():
            while True:
                msg = self.router.recv_multipart()
                print(f'msg: {msg}')
                if len(msg) >= 2:
                    if str(msg[1]) == 'hello' or str(msg[1] == 'heartbeat'):
                        # register/update ts
                        mClients[msg[0]] = time.time()
                        if not self.started:
                            qRcv.put('start')
                    elif str(msg[1]) == 'stop':
                        stop = True
                        for k in mClients:
                            if mClients[k] - time.time < self.HEARTBEAT:
                                stop = False
                                break
                        if stop:
                            qRcv.put('stop')
                    else:
                        print(f'unkown msg {msg[1]}')
                else:
                    print(f'invalid msg: {len(msg)}')

        def send():
            while True:
                msg = qSend.get()
                parts = ['', msg]
                for k in mClients:
                    # 5 minutes heartbeat
                    if mClients[k] - time.time() < self.HEARTBEAT:
                        parts[0] = k
                        try:
                            self.router.send_multipart(parts)
                        except Exception as e:
                            print(f'exception send: {e}')

        threading.Thread(target=rcv).start()
        threading.Thread(target=send).start()
        print('started mq')


def on_message(ws, message):
    #  {"end_time":62750,"err_msg":"OK","err_no":0,"log_id":530385024,"result":"语音识别。","sn":"e9046d79-ec33-443a-adc0-cdc5791764d8_ws_5","start_time":60860,"type":"FIN_TEXT"}
    print("on msg: ", message)
    msg = json.loads(message)
    if 'err_no' in msg and msg['err_no'] == 0 and msg['type'] == 'FIN_TEXT':
        result = msg['result']
        print('type of result: ', type(result))
        qSend.put(bytes(result, 'utf-8'))


def on_error(ws, error):
    print(error)
    ws_connect()


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    def run(*args):
        print('ws connected')
        FPB = 16*2000  # buffer 2s
        audio = pyaudio.PyAudio()
        stream = audio.open(format=pyaudio.paInt16, channels=1, rate=16000,
                            input_device_index=None, input=True,  # output=True,
                            frames_per_buffer=FPB)

        fin = json.dumps({
            "type": "FINISH"
        })

        state = ''

        while True:
            # baidu rt-asr spec: every 160ms(16frames per 1ms) audio slice
            try:
                msg = qRcv.get_nowait()
            except:
                msg = None

            if msg == 'start' and state != 'started':
                ws.send(json.dumps(start_data), websocket.ABNF.OPCODE_TEXT)
                state = 'started'

            # send audio stream
            if state == 'started':
                bytes = stream.read(16*160, exception_on_overflow=False)
                ws.send(bytes, websocket.ABNF.OPCODE_BINARY)
            elif msg == 'stop':
                ws.send(fin, websocket.ABNF.OPCODE_TEXT)
                state = ''
            else:
                time.sleep(0.5)

    threading.Thread(target=run).start()
    print("worker started")


def ws_connect():
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    th = threading.Thread(target=ws.run_forever)
    th.start()
    th.join()


if __name__ == "__main__":
    mq = MQ()
    mq()
    ws_connect()
