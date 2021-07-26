# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath('./utils'))
from utils.time_utils import time_utils_random_rfc3339_time
from utils.time_utils import time_utils_random_timestamp

from flask import Flask, jsonify, request
time_app = Flask(__name__)
time_app.config.from_object(__name__)


@time_app.route("/keepalive", methods=["GET"])
def keep_alive():
    response_object = {"status": "ALIVE"}
    return jsonify(response_object)


@time_app.route("/random_rfc3339_time", methods=["GET"])
def random_rfc3339_time():
    response_object = {"status": "OK"}
    # here we want to get the value of passed (i.e. ?passed=14)
    passed_days = request.args.get("passed", 30, type=int)
    response_object["time"] = time_utils_random_rfc3339_time(passed_days)
    return jsonify(response_object)


@time_app.route("/random_timestamp", methods=["GET"])
def random_timestamp():
    response_object = {"status": "OK"}
    # here we want to get the value of passed (i.e. ?passed=14)
    passed_days = request.args.get("passed", 30, type=int)
    response_object["time"] = time_utils_random_timestamp(passed_days)
    return jsonify(response_object)
