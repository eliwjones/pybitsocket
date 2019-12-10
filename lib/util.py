import base64
import json
from concurrent import futures

import zmq.green as zmq
from gevent.queue import Empty
from pybtc import Transaction


def decode_tx(message):
    _type, raw_tx, _id = message

    decoded_tx = json.dumps(Transaction(raw_tx=raw_tx), indent=4)

    return decoded_tx


def event_stream(q):
    while True:
        try:
            message = q.get(timeout=0.1)
        except Empty:
            continue

        message = message.replace('\n', '\ndata:')
        yield f"data: {message}\n\n"


def normalize_b64(doc):
    b64_doc = json.loads(base64.b64decode(doc))
    sort_all_lists(b64_doc)

    b64_doc = base64.b64encode(json.dumps(b64_doc, sort_keys=True).encode('utf-8'))

    return b64_doc


def pool_stream(tx_stream):
    """
      Pull 5 items at a time, and map them to a process pool.

      This should help us use up available CPU cores.
    """
    pool = futures.ProcessPoolExecutor()
    while True:
        """
          This list comprehension feels too clever, but it's neat so I'll let it stay for a bit.
        """
        batch = [m for _, m in zip(range(5), tx_stream)]
        for message in pool.map(decode_tx, batch):
            yield message


def rawtx_stream():
    context = zmq.Context()

    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 10)
    socket.setsockopt(zmq.SUBSCRIBE, b"rawtx")

    socket.connect("tcp://127.0.0.1:28332")

    while True:
        message = socket.recv_multipart()
        print(f"[rawtx_stream] yielding {message}")
        yield message


def sort_all_lists(obj):
    for key, value in obj.items():
        if isinstance(value, list):
            value.sort(key=lambda item: json.dumps(item, sort_keys=True))
        if isinstance(value, dict):
            sort_all_lists(value)
