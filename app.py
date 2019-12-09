import base64
import json

from queue import Queue, Empty
from flask import Flask, render_template, request, Response

from lib import util

app = Flask(__name__)
app.debug = True

QUEUES = {}
RAWTX = util.rawtx_subscriber()

topic, body, seq = RAWTX.recv_multipart()


@app.route('/')
def hello():
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

    return Response(event_stream(q), mimetype="text/event-stream")


@app.route('/api/post', methods=['GET'])
def api_parse_sentence():
    b64_doc = util.normalize_b64(request.args.get('b64_doc'))

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
