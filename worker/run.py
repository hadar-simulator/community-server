import os
from time import sleep

import requests
import hadar as hd
from models import JobDTO


class Client:
    def __init__(self, host: str):
        self.base = host

    def get_next_job(self) -> JobDTO:
        try:
            r = requests.get('%s/api/v1/job/next/%s' % (self.base, hd.__version__)).json()
            return None if r == {} else JobDTO.from_json(r)
        except requests.ConnectionError:
            print('Failed to connect to %s' % self.base)

    def send_job(self, job: JobDTO) -> str:
        url = '%s/api/v1/job/%s' % (self.base, job.id)
        r = requests.post(url, json=job.to_json())
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
            res = optim.solve(hd.Study.from_json(job.study))
            job.result = res.to_json()
            print('Finish job:', job.id)
        except Exception as e:
            job.status = 'ERROR'
            job.error = str(e)
            print('Error on job', job.id, ':', e)
        finally:
            return client.send_job(job)
    else:
        sleep(1)


if __name__ == '__main__':
    url = os.getenv('SCHEDULER_URL', 'http://localhost:8765')
    client = Client(url)
    print('Worker started with hadar version', hd.__version__)

    while True:
        compute(client)
