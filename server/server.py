import os
import pickle
import queue
import threading
from time import sleep

from flask import Flask, request, abort
import hadar as hd

from storage import JobRepository, Job, sha256


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
        try:
            res = optim.solve(job.study)
            job.status = 'TERMINATED'
            job.result = res
        except Exception as e:
            job.status = 'ERROR'
            job.error = str(e)
        finally:
            repo.save(job)


def garbage():
    """
    Delete every minute all job tagged TERMINATED.

    :return:
    """
    repo = JobRepository()
    while True:
        repo.delete_terminated()
        sleep(int(os.getenv('GARBAGE_LOOP_SEC', '60')))


def create_app():
    """
    Create Flask app. Start threads.

    :return: Flask app
    """
    app = Flask(__name__)
    threading.Thread(target=schedule).start()
    threading.Thread(target=garbage).start()
    return app


def auth():
    token = os.environ.get('ACCESS_TOKEN', None)
    if token is not None and sha256(request.args['token'].encode()) != sha256(token.encode()):
        abort(403, 'Wrong access token given')


todo = queue.Queue()
application = create_app()


@application.route("/study", methods=['POST'])
def send_study():
    """
    Receive study, put into queue and respond with study id.

    :return:
    """
    auth()

    print('Receive study', end=' ')
    repo = JobRepository()
    study = pickle.loads(request.data)
    job = Job(study)
    if repo.get(job.id) is None:
        repo.save(job)
        todo.put(job)

    return pickle.dumps({'job': job.id, 'status': 'QUEUED', 'progress': repo.count_jobs_before(job)})


@application.route("/result/<job_id>", methods=['GET'])
def get_result(job_id: str):
    """
    Check if job is terminated, respond with result in this case.

    :param job_id: job is to check
    :return: just job status or status + result if job terminated
    """
    # auth()

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


if __name__ == '__main__':
    application.run(debug=True, host='0.0.0.0', port=5002)

