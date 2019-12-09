import base64
import json
import zmq.green as zmq

from gevent.queue import Empty


def event_stream(q):
    while True:
        try:
            message = q.get(timeout=0.1)
        except Empty:
            continue

        print(f"Sending: {message}")
        yield f"data: {message}\n\n"


def normalize_b64(doc):
    b64_doc = json.loads(base64.b64decode(doc))
    sort_all_lists(b64_doc)

    b64_doc = base64.b64encode(json.dumps(b64_doc, sort_keys=True).encode('utf-8'))

    return b64_doc


def rawtx_receiver():
    context = zmq.Context()

    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 10)
    socket.setsockopt(zmq.SUBSCRIBE, b"rawtx")

    socket.connect("tcp://127.0.0.1:28332")

    while True:
        yield socket.recv_multipart()


def sort_all_lists(obj):
    for key, value in obj.items():
        if isinstance(value, list):
            value.sort(key=lambda item: json.dumps(item, sort_keys=True))
        if isinstance(value, dict):
            sort_all_lists(value)
