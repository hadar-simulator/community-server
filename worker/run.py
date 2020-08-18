import os
import pickle
from time import sleep

import requests
import hadar as hd
from models import JobDTO


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
    delay = int(os.getenv('DELAY_S', 0))
    client = Client(url)
    sleep(delay)
    print('Worker started with hadar version', hd.__version__)

    while True:
        compute(client)
