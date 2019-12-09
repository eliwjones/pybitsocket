import json
import zmq
# import zmq.green as zmq


def normalize_b64(doc):
    b64_doc = json.loads(base64.b64decode(doc))
    sort_all_lists(b64_doc)

    b64_doc = base64.b64encode(json.dumps(b64_doc, sort_keys=True).encode('utf-8'))


def event_stream(q):
    while True:
        try:
            message = q.get(timeout=0.1)
        except Empty:
            continue

        print(f"Sending: {message}")
        yield f"data: {message}\n\n"

@context()
@socket(CONNECTION_TYPE)
def rawtx_subscriber():
    address = "tcp://127.0.0.1:28332"
    zmq_context = zmq.Context()
    socket = zmq_context.socket(zmq.SUB)
    socket.set(zmq.RCVTIMEO, 60000)
    socket.connect(address)
    socket.setsockopt(zmq.SUBSCRIBE, b"rawtx")
    return socket


def sort_all_lists(obj):
    for key, value in obj.items():
        if isinstance(value, list):
            value.sort(key=lambda item: json.dumps(item, sort_keys=True))
        if isinstance(value, dict):
            sort_all_lists(value)
