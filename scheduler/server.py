import hashlib
import os
import pickle
import time

from flask import Flask, request, abort, render_template

from models import JobDTO
from scheduler.storage import JobRepository
import scheduler


def sha256(array):
    m = hashlib.sha256()
    m.update(array)
    return m.hexdigest()


def auth():
    token = os.environ.get('ACCESS_TOKEN', None)
    if token is not None and sha256(request.args['token'].encode()) != sha256(token.encode()):
        abort(403, 'Wrong access token given')


repo = JobRepository()  # Start before everyone to create table
application = Flask(__name__)


@application.route('/', methods=['GET'])
def home():
    host = request.environ.get('HTTP_HOST', '')
    token = 'ACCESS_TOKEN' in os.environ

    return render_template('home.html', version=scheduler.__version__, host=host, token=token)


@application.route("/study", methods=['POST'])
def receive_study():
    """
    Receive study, put into queue and respond with study id.

    :return:
    """
    print('Receive study', end=' ')
    auth()

    repo = JobRepository()
    # garbage data
    timeout = int(os.getenv('DATA_EXPIRATION_MS', 24 * 60 * 60 * 1000))  # Keep 24h by default
    repo.delete_terminated(timeout)

    study = request.data
    job = JobDTO(study=study, version='1')

    if repo.get(job.id) is None:
        repo.save(job)

    return pickle.dumps({'job': job.id, 'status': 'QUEUED', 'progress': max(1, repo.count_jobs_before(job))})


@application.route("/result/<job_id>", methods=['GET'])
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
        return pickle.dumps({'status': job.status, 'progress': repo.count_jobs_before(job)})
    elif job.status in 'COMPUTING':
        return pickle.dumps({'status': job.status, 'progress': 0})
    elif job.status == 'TERMINATED':
        return pickle.dumps({'status': job.status, 'result': job.result})
    elif job.status == 'ERROR':
        return pickle.dumps({'status': job.status, 'message': job.error})


@application.route('/job/next', methods=['GET'])
def get_next_job():
    """
    Get next job available to compute.

    :return:
    """
    auth()

    repo = JobRepository()
    job = repo.get_next()
    if job:
        job.status = 'COMPUTING'
        job.computed = int(time.time() * 1000)
        repo.save(job)
        return pickle.dumps(job)
    else:
        return pickle.dumps(None)


@application.route('/job/<id>', methods=['POST'])
def update_job(id: int):
    auth()

    repo = JobRepository()
    job = pickle.loads(request.data)
    job.status = 'TERMINATED'
    job.terminated = int(time.time() * 1000)
    repo.save(job)
    return id, 200


if __name__ == '__main__':
    application.run(debug=False, host='0.0.0.0', port=8765)

