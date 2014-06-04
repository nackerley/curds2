#
"""
Flask app to service dbapi2 Antelope requests using curds2
"""
import os
from flask import Flask, request, jsonify
from curds2.ws.service import Service

PORT=5150
app = Flask(__name__)


def process_request(request):
    """Turn a flask POST request into a request for the service"""
    if request.method == 'POST':
        req = request.get_json()
        return req
    else:
        return {}

def process_reply(rep):
    """
    Turn a service reply into a flask JSON response
    """
    return jsonify(rep)


@app.route('/<path:dbname>', methods=['GET', 'POST'])
def curds_service(dbname):
    dbname = os.path.join(os.sep, dbname)
    req = process_request(request)
    result = Service(dbname).run(req)
    return process_reply(result)


# Main routines -- for standalone web servers
def gevent_main():
    """
    Gevent coroutine WDGI standalone server
    """
    from gevent.wsgi import WSGIServer
    http_server = WSGIServer(('', PORT), app)
    http_server.serve_forever()


def dev_main():
    """
    Flask dev standalone server
    ---------------------------
    host='0.0.0.0' to listen on non-local ips
    debug=True to use debugger (NEVER in production!!)
    """
    app.run(port=PORT, debug=True)


if __name__=="__main__":
    dev_main()
