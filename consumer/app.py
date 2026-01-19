import base64
import json
import logging
import os

from flask import Flask, request

app = Flask(__name__)
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))


@app.get('/listening')
def listening_check():
    return 'Consumer service is listening', 200


@app.post('/push')
def pubsub_push():
    envelope = request.get_json(silent=True) or {}
    msg = envelope.get('message') or {}
    b64 = msg.get('data')
    if not b64:
        logging.warning('No message.data in request: %s', envelope)
        return ('Bad Request: no message.data', 400)

    try:
        raw = base64.b64decode(b64).decode('utf-8')
        obj = json.loads(raw)
    except Exception:
        logging.exception('Failed to decode/parse Pub/Sub message')
        return ('Bad Request: invalid message.data', 400)

    logging.info('Received message: %s', obj)
    return ('', 204)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '8080')))
