import hashlib
import json
import os
import time

from flask import Flask, request, abort, render_template
from flask_cors import CORS, cross_origin

from models import JobDTO
from scheduler.storage import JobRepository
import scheduler


def sha256(array):
    m = hashlib.sha256()
    m.update(array)
    return m.hexdigest()


def auth():
    if TOKEN is not None and sha256(request.args['token'].encode()) != sha256(TOKEN.encode()):
        abort(403, 'Wrong access token given')


JobRepository()  # Start before everyone to create table
application = Flask(__name__)
CORS(application)

INTERN_ORIGIN = os.environ.get('INTERN_ORIGIN', '*')
TOKEN = os.environ.get('ACCESS_TOKEN', None)
DATA_EXPIRATION = int(os.getenv('DATA_EXPIRATION_MS', 24 * 60 * 60 * 1000))  # Keep 24h by default
VERSION = scheduler.__version__


@application.route('/', methods=['GET', 'OPTION'])
@cross_origin(origin='*')
def home():
    host = request.environ.get('HTTP_HOST', '')

    return render_template('home.html', version=VERSION, host=host, token=TOKEN is not None)


@application.route("/api/v1/study", methods=['POST', 'OPTION'])
@cross_origin(origin='*')
def receive_study():
    """
    Receive study, put into queue and respond with study id.

    :return:
    """
    print('Receive study', end=' ')
    auth()
    repo = JobRepository()

    # garbage data
    repo.delete_terminated(DATA_EXPIRATION)

    study = json.loads(request.data)
    job = JobDTO(study=study)

    if not repo.exists(job.id):
        repo.save(job)

    return json.dumps({'job': job.id, 'status': 'QUEUED', 'progress': max(1, repo.count_jobs_before(job))})


@application.route("/api/v1/result/<job_id>", methods=['GET', 'OPTION'])
@cross_origin(origin='*')
def get_result(job_id: str):
    """
    Check if job is terminated, respond with result in this case.

    :param job_id: job is to check
    :return: just job status or status + result if job terminated
    """
    auth()

    repo = JobRepository()
    job = repo.get(job_id)
    if job is None:
        abort(404, 'Job id not found')

    elif job.status == 'QUEUED':
        return json.dumps({'status': job.status, 'progress': repo.count_jobs_before(job)})
    elif job.status in 'COMPUTING':
        return json.dumps({'status': job.status, 'progress': 0})
    elif job.status == 'TERMINATED':
        return json.dumps({'status': job.status, 'result': job.result})
    elif job.status == 'ERROR':
        return json.dumps({'status': job.status, 'message': job.error})


@application.route('/api/v1/job/next/<version>', methods=['GET', 'OPTION'])
@cross_origin(origin=INTERN_ORIGIN)
def get_next_job(version: str):
    """
    Get next job available to compute.

    :return:
    """

    repo = JobRepository()
    job = repo.get_next(version)
    if job:
        job.status = 'COMPUTING'
        job.computed = int(time.time() * 1000)
        repo.save(job)
        return json.dumps(job.to_json())
    else:
        return json.dumps({})


@application.route('/api/v1/job/<id>', methods=['POST', 'OPTION'])
@cross_origin(origin=INTERN_ORIGIN)
def update_job(id: int):

    repo = JobRepository()
    job = JobDTO.from_json(json.loads(request.data))
    job.status = 'TERMINATED'
    job.terminated = int(time.time() * 1000)
    repo.save(job)
    return id, 200


if __name__ == '__main__':
    application.run(debug=False, host='0.0.0.0', port=8765)

