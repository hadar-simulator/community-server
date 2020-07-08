import base64
import hashlib
import os
import pickle
import sqlite3
import threading
from time import sleep
from typing import List

from flask import Flask, request, abort
import hadar as hd


def schedule():
    while True:
        print('remain job=', queue.jobs)
        next = queue.get_next_study()
        if next:
            optim = hd.LPOptimizer()
            res = optim.solve(next)
            sleep(1)
            queue.set_result(res)
        sleep(2)


def get_hash(array):
    m = hashlib.sha256()
    m.update(array)
    return m.hexdigest()


def create_app():
    app = Flask(__name__)
    threading.Thread(target=schedule).start()
    return app


queue = Queue()
application = create_app()


@application.route("/study", methods=['POST'])
def send_study():
    token = os.environ.get('ACCESS_TOKEN', None)
    if token is not None and request.args['token'] != token:
        abort(403, 'Wrong access token given')

    print('Receive study...', end=' ')
    id = get_hash(request.data)
    study = pickle.loads(request.data)
    queue.set_study(study, id)

    return pickle.dumps({'job': id, 'status': 'QUEUED'})


@application.route("/result/<id>", methods=['POST'])
def get_result(id: int):
    token = os.environ.get('ACCESS_TOKEN', None)
    if token is not None and request.args['token'] != token:
        abort(403, 'Wrong access token given')

    job = queue.get_job(id)
    if job is None:
        abort(404, 'Job id not found')
    if job.status in ['QUEUED', 'COMPUTING']:
        return pickle.dumps({'status': job.status})
    elif job.status == 'TERMINATED':
        return pickle.dumps({'status': job.status, 'result': job.get_result()})


if __name__ == '__main__':
    application.run(debug=True, host='0.0.0.0', port=5001)

