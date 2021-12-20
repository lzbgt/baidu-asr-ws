from os import stat
import websocket
import threading
import time
import json
import pyaudio
import zmq
import decouple
import queue
import logging
import traceback

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger("some.logger")

url = decouple.config(
    'api', 'wss://vop.baidu.com/realtime_asr?sn=e9046d79-ec33-443a-adc0-cdc5791764d8')

# hint: swapid
start_data = {
    "type": "START",
    "data": {
        "appid": int(decouple.config('appid', 15299032)),
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
                log.info(f'msg: {msg}')
                if len(msg) >= 2:
                    dmsg = bytes(msg[1]).decode()
                    if dmsg == str('hello') or dmsg == str('start') or (dmsg == 'heartbeat'):
                        # register/update ts
                        mClients[msg[0]] = time.time()
                        qRcv.put('start')
                    elif bytes(msg[1]).decode() == str('stop'):
                        stop = True
                        for k in mClients:
                            if mClients[k] - time.time() < self.HEARTBEAT and (str(k) != str(msg[0])):
                                stop = False
                                break
                        if stop:
                            log.info('stop received\n\n\n')
                            qRcv.put('stop')
                    else:
                        log.error('unkown msg: %s', msg)
                else:
                    log.error('invalid msg: %s', msg)

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
                            log.error(f'exception send: {e}')

        threading.Thread(target=rcv, name='mq_rcv').start()
        threading.Thread(target=send, name='mq_send').start()
        log.info('started mq')


class Worker:
    def _run(self, ws: websocket.WebSocketApp):
        log.info('ws connected')
        FPB = 16*16000  # buffer 16s
        audio = pyaudio.PyAudio()
        stream = audio.open(format=pyaudio.paInt16, channels=1, rate=16000,
                            input_device_index=None, input=True,  # output=True,
                            frames_per_buffer=FPB)

        fin = json.dumps({
            "type": "FINISH"
        })

        hb = json.dumps({
            "type": "HEARTBEAT"
        })

        state = ''

        while True:
            # baidu rt-asr spec: every 160ms(16frames per 1ms) audio slice
            try:
                msg = qRcv.get_nowait()
            except:
                msg = None

            if msg != None:
                log.info('mqtt cmd: %s', msg)

            if msg == 'stop' and state == 'started':
                ws.send(fin, websocket.ABNF.OPCODE_TEXT)
                state = ''
                log.info('stopping')
                break

            if msg == 'start' and state != 'started':
                log.info('parent asr start pkt')
                try:
                    ws.send(json.dumps(start_data),
                            websocket.ABNF.OPCODE_TEXT)
                    state = 'started'
                except:
                    log.error(traceback.format_exc())
                    break

            # send audio stream
            if state == 'started':
                bytes = stream.read(16*160, exception_on_overflow=False)
                try:
                    ws.send(bytes, websocket.ABNF.OPCODE_BINARY)
                except:
                    log.error(traceback.format_exc())
                    break
            else:
                # not started
                time.sleep(1)

            # else:
            #     try:
            #         ws.send(hb, websocket.ABNF.OPCODE_TEXT)
            #         time.sleep(0.5)
            #     except:
            #         log.error(traceback.format_exc())
            #         break

    def __init__(self, ws: websocket.WebSocketApp):
        try:
            threading.Thread(target=self._run, args=(ws,),
                             name='worker').start()
        except:
            traceback.print_exc()


def on_message(ws, message):
    #  {"end_time":62750,"err_msg":"OK","err_no":0,"log_id":530385024,"result":"语音识别。","sn":"e9046d79-ec33-443a-adc0-cdc5791764d8_ws_5","start_time":60860,"type":"FIN_TEXT"}
    # log.info("on msg: %s", message)
    # if 'err_no' in message and message['err_no'] != 0:
    #     log.error('on error msg: %s', message)
    msg = json.loads(message)
    if 'err_no' in msg and msg['err_no'] == 0 and msg['type'] == 'FIN_TEXT':
        result = msg['result']
        log.info("result: %s", result)
        qSend.put(bytes(result, 'utf-8'))
    elif 'err_no' in msg and msg['err_no'] == -3102:
        ws.close()


def on_error(ws, error):
    log.error(error)
    # ws_connect()


def on_close(ws, close_status_code, close_msg):
    log.info("### closed ###")
    # ws_connect()


def on_open(ws: websocket.WebSocketApp):
    log.info('on open %s', ws)
    Worker(ws)


def ws_connect():
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    th = threading.Thread(target=ws.run_forever(), name='ws')
    th.start()
    return (ws, th)


if __name__ == "__main__":
    mq = MQ()
    mq()
    while True:
        ws_connect()[1].join()
