# 百度实时语音识别

### main.py 主程序
1. 实时捕捉当前运行电脑的默认Microphone输入， 并流式传输到百度语音识别
2. 识别结果广播到ZMQ Router注册的所有dealer， zmq 地址tcp://127.0.0.1:2123

### client.py
测试zmq dealer 客户端
其他语言的实现可以参考该客户端

## 运行环境
1. python3.9.x
2. pip install -r requirements.txt
3. python main.py
4. python client.py


