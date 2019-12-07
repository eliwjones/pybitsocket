import base64
import json

from queue import Queue, Empty
from flask import Flask, render_template, request, Response

import zmq
# import zmq.green as zmq

app = Flask(__name__)
app.debug = True

QUEUES = {}


def rawtx_subscriber():
    address = "tcp://127.0.0.1:28332"
    zmq_context = zmq.Context()
    socket = zmq_context.socket(zmq.SUB)
    socket.set(zmq.RCVTIMEO, 60000)
    socket.connect(address)
    socket.setsockopt(zmq.SUBSCRIBE, b"rawtx")
    return socket


RAWTX = rawtx_subscriber()

topic, body, seq = RAWTX.recv_multipart()


def sort_all_lists(obj):
    for key, value in obj.items():
        if isinstance(value, list):
            value.sort(key=lambda item: json.dumps(item, sort_keys=True))
        if isinstance(value, dict):
            sort_all_lists(value)


def event_stream(q):
    while True:
        try:
            message = q.get(timeout=0.1)
        except Empty:
            continue

        print(f"Sending: {message}")
        yield f"data: {message}\n\n"


@app.route('/')
def hello():
    return render_template('index.html')


@app.route('/s/<b64_query>')
def stream(b64_query):
    print(f"[s] b64_query: {b64_query}")
    """
      Poor man's normalization of queries.
    """
    query = json.loads(base64.b64decode(b64_query))
    sort_all_lists(query)

    b64_query = base64.b64encode(json.dumps(query, sort_keys=True).encode('utf-8'))

    if b64_query not in QUEUES:
        print(f"[s] adding query: {b64_query} to queue. query: {json.loads(base64.b64decode(b64_query))}")

        QUEUES[b64_query] = []

    q = Queue()
    QUEUES[b64_query].append(q)

    return Response(event_stream(q), mimetype="text/event-stream")


@app.route('/api/post', methods=['GET'])
def api_parse_sentence():
    b64_doc = json.loads(base64.b64decode(request.args.get('b64_doc')))
    sort_all_lists(b64_doc)

    b64_doc = base64.b64encode(json.dumps(b64_doc, sort_keys=True).encode('utf-8'))

    for b64_query in QUEUES:
        """
          Just imagine this is a filter check.

          Right now, we are just detecting a direct match.
        """
        if b64_query != b64_doc:
            continue

        for q in QUEUES[b64_query]:
            q.put(f"matched {b64_doc}")

    return "OK"


if __name__ == '__main__':
    """
      TODO: replace threaded with regular gunicorn with gevent.
    """
    app.run(host='0.0.0.0', port=3000, debug=True, threaded=True)
