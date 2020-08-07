import os
import pickle
from time import sleep

import requests
import hadar as hd

class Job:
    """
    Entity stored in db.
    """
    def __init__(self, study: hd.Study,
                 id: str = None,
                 created: int = None,
                 status: str = 'QUEUED',
                 result: hd.Result = None,
                 error: str = ''):
        self.study = study
        self.created = created
        self.status = status
        self.id = id
        self.result = result
        self.error = error


class Client:
    def __init__(self, url: str):
        self.base = url

    def get_next_job(self) -> Job:
        url = '%s/job/next' % self.base
        r = requests.get(url)
        return pickle.loads(r.content)

    def send_job(self, job: Job) -> str:
        url = '%s/job/%s' % (self.base, job.id)
        data = pickle.dumps(job)
        r = requests.post(url, data=data, headers={'Content-Length': str(len(data))})
        return r.content.decode('ascii')


def compute(client: Client):
    """
    Wait job from queue. Start compute. Store any changing job state during process.

    :return:
    """

    job = client.get_next_job()
    if job:
        try:
            print('Start job:', job.id)
            optim = hd.LPOptimizer()
            res = optim.solve(job.study)
            job.result = res
            print('Finish job:', job.id)
        except Exception as e:
            job.status = 'ERROR'
            job.error = str(e)
            print('Error on job:', job.id)
        finally:
            return client.send_job(job)
    else:
        sleep(1)
        return None


if __name__ == '__main__':
    url = os.getenv('SCHEDULER_URL', 'http://localhost:8765')
    client = Client(url)
    while True:
        compute(client)
