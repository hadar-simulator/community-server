import unittest
from time import sleep

import hadar as hd

from schelduler.storage import JobRepository, Job


class TestJobRepository(unittest.TestCase):

    def setUp(self) -> None:
        self.repo = JobRepository(path=':memory:')

    def test_db(self):
        study = hd.Study(horizon=1)\
            .network().node('a').consumption(name='load', cost=10, quantity=10).build()
        job = Job(study=study, error='wrong')

        self.repo.save(job)

        job = self.repo.get(job.id)
        self.assertIsNotNone(job.id)
        self.assertTrue(job.created > 0)
        self.assertEqual(study, job.study)
        self.assertEqual('QUEUED', job.status)
        self.assertIsNone(job.result)
        self.assertEqual('wrong', job.error)

    def test_delete_terminated(self):
        t = self.repo.save(Job(study='aaa', status='TERMINATED'))
        e = self.repo.save(Job(study='bbb', status='ERROR'))
        q = self.repo.save(Job(study='ccc', status='QUEUED'))

        self.repo.delete_terminated()

        self.assertIsNone(self.repo.get(t))
        self.assertIsNone(self.repo.get(e))
        self.assertIsNotNone(self.repo.get(q))

    def test_count_jobs_before(self):
        self.repo.save(Job(study='111', status='QUEUED'))
        sleep(0.1)
        self.repo.save(Job(study='222', status='QUEUED'))
        sleep(0.1)
        j = self.repo.save(Job(study='333', status='QUEUED'))
        sleep(0.1)
        self.repo.save(Job(study='444', status='QUEUED'))

        job = self.repo.get(j)
        self.assertEqual(2, self.repo.count_jobs_before(job))

    def test_get_next(self):
        first = self.repo.save(Job(study='111', status='QUEUED'))
        sleep(0.1)
        self.repo.save(Job(study='222', status='COMPUTING'))
        sleep(0.1)
        second = self.repo.save(Job(study='333', status='QUEUED'))
        sleep(0.1)
        self.repo.save(Job(study='444', status='TERMINATED'))

        job = self.repo.get_next()
        self.assertEqual(first, job.id)

        job.status = 'TERMINATED'
        self.repo.save(job)

        job = self.repo.get_next()
        self.assertEqual(second, job.id)
