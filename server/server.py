import os
import pickle
import queue
import threading
from time import sleep

from flask import Flask, request, abort
import hadar as hd

from storage import JobRepository, Job


def schedule():
    """
    Wait job from queue. Start compute. Store any changing job state during process.

    :return:
    """
    optim = hd.LPOptimizer()
    repo = JobRepository()
    while True:
        print('Ready for next job')
        job = todo.get()
        job.status = 'COMPUTING'
        repo.save(job)

        res = optim.solve(job.study)
        sleep(2)
        job.status = 'TERMINATED'
        job.result = res
        repo.save(job)


def garbage():
    """
    Delete every minute all job tagged TERMINATED.

    :return:
    """
    repo = JobRepository()
    while True:
        repo.delete_terminated()
        sleep(60)


def create_app():
    """
    Create Flask app. Start threads.

    :return: Flask app
    """
    app = Flask(__name__)
    threading.Thread(target=schedule).start()
    threading.Thread(target=garbage).start()
    return app


todo = queue.Queue()
application = create_app()


@application.route("/study", methods=['POST'])
def send_study():
    """
    Receive study, put into queue and respond with study id.

    :return:
    """
    token = os.environ.get('ACCESS_TOKEN', None)
    if token is not None and request.args['token'] != token:
        abort(403, 'Wrong access token given')

    print('Receive study', end=' ')
    repo = JobRepository()
    study = pickle.loads(request.data)
    job = Job(study)
    if repo.get(job.id) is None:
        repo.save(job)
        todo.put(job)

    return pickle.dumps({'job': job.id, 'status': 'QUEUED'})


@application.route("/result/<job_id>", methods=['GET'])
def get_result(job_id: str):
    """
    Check if job is terminated, respond with result in this case.

    :param job_id: job is to check
    :return: just job status or status + result if job terminated
    """
    token = os.environ.get('ACCESS_TOKEN', None)
    if token is not None and request.args['token'] != token:
        abort(403, 'Wrong access token given')

    repo = JobRepository()
    print('id=', job_id)
    job = repo.get(job_id)
    if job is None:
        abort(404, 'Job id not found')
    if job.status in ['QUEUED', 'COMPUTING']:
        return pickle.dumps({'status': job.status})
    elif job.status == 'TERMINATED':
        return pickle.dumps({'status': job.status, 'result': job.result})


if __name__ == '__main__':
    application.run(debug=True, host='0.0.0.0', port=5002)

