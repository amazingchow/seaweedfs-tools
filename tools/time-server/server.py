# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath('./app'))
from app.time_app import time_app

from gevent.pywsgi import WSGIServer


if __name__ == "__main__":
    http_server = WSGIServer(("", 5000), time_app)
    http_server.serve_forever()
