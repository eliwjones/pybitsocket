import base64
import gevent
import json

from gevent.queue import Queue
from flask import Flask, render_template, Response

from lib import util

app = Flask(__name__)
app.debug = True

QUEUES = {}


def tx_consumer():
    rawtx_receiver = util.rawtx_receiver()
    for message in rawtx_receiver:
        for query in QUEUES:
            """
              Determine if message potentially matches query filter.
            """
            match = True
            if not match:
                continue

            for q in QUEUES[query]:
                q.put(message)


gevent.spawn(tx_consumer)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/s/<b64_query>')
def stream(b64_query):
    print(f"[s] b64_query: {b64_query}")

    b64_query = util.normalize_b64(b64_query)

    if b64_query not in QUEUES:
        print(f"[s] adding query: {b64_query} to queue. query: {json.loads(base64.b64decode(b64_query))}")

        QUEUES[b64_query] = []

    q = Queue()
    QUEUES[b64_query].append(q)

    return Response(util.event_stream(q), mimetype="text/event-stream")


if __name__ == '__main__':
    """
      TODO: replace threaded with regular gunicorn with gevent.
      
      gunicorn --worker-class=gevent --workers=1 --bind=0.0.0.0:3000 --log-level=debug app:app
    """
    app.run(host='0.0.0.0', port=3000, debug=True)
