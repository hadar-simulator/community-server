import os
import pickle
from time import sleep

import requests
import hadar as hd


class JobDTO:
    """
    Entity stored in db.
    """
    def __init__(self, study: bytes,
                 version: str,
                 created: int = 0,
                 computed: int = 0,
                 terminated: int = 0,
                 id: str = None,
                 status: str = 'QUEUED',
                 result: bytes = None,
                 error: str = ''):
        self.created = created
        self.computed = computed
        self.terminated = terminated
        self.status = status
        self.id = id
        self.version = version
        self.error = error
        self.study = study
        self.result = result
        self.id = id


class Client:
    def __init__(self, url: str):
        self.base = url

    def get_next_job(self) -> JobDTO:
        url = '%s/job/next' % self.base
        try:
            r = requests.get(url)
            return pickle.loads(r.content)
        except requests.ConnectionError:
            print('Failed to connect to %s' % self.base)
            return None

    def send_job(self, job: JobDTO) -> str:
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
            res = optim.solve(pickle.loads(job.study))
            job.result = pickle.dumps(res)
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
