import os
import shutil
import unittest
from time import sleep, time

from scheduler.storage import JobRepository, JobDTO


class TestJobRepository(unittest.TestCase):

    def setUp(self) -> None:
        self.repo = JobRepository(path='data')

    def tearDown(self) -> None:
        if os.path.exists('data'):
            shutil.rmtree('data')

    def test_db(self):
        job = JobDTO(study='Hello, World', version='1', error='wrong', result='Bonjour le Monde')

        self.repo.save(job)

        self.assertTrue(self.repo.exists(job.id))
        self.assertFalse(self.repo.exists('123'))

        job = self.repo.get(job.id)
        self.assertIsNotNone(job.id)
        self.assertTrue(job.created > 0)
        self.assertEqual('Hello, World', job.study)
        self.assertEqual('QUEUED', job.status)
        self.assertEqual('Bonjour le Monde', job.result)
        self.assertEqual('wrong', job.error)
        self.assertTrue(os.path.exists('data/studies/%s' % job.id))
        self.assertTrue(os.path.exists('data/results/%s' % job.id))

    def test_delete_terminated(self):
        now = int(time() * 1000)
        t = self.repo.save(JobDTO(study='aaa', version='1', terminated=now, status='TERMINATED'))
        e = self.repo.save(JobDTO(study='bbb', version='1', terminated=now, status='ERROR'))
        q = self.repo.save(JobDTO(study='ccc', version='1', terminated=now, status='QUEUED'))

        # Delete before timeout
        self.repo.delete_terminated(timeout=1000)
        self.assertIsNotNone(self.repo.get(t))
        self.assertIsNotNone(self.repo.get(e))
        self.assertIsNotNone(self.repo.get(q))

        sleep(1)
        # Delete after timeout
        self.repo.delete_terminated(timeout=1000)
        self.assertIsNone(self.repo.get(t))
        self.assertIsNone(self.repo.get(e))
        self.assertIsNotNone(self.repo.get(q))

    def test_count_jobs_before(self):
        self.repo.save(JobDTO(study='111', version='1', status='QUEUED'))
        sleep(0.1)
        self.repo.save(JobDTO(study='222', version='1',  status='QUEUED'))
        sleep(0.1)
        j = self.repo.save(JobDTO(study='333', version='1',  status='QUEUED'))
        sleep(0.1)
        self.repo.save(JobDTO(study='444', version='1',  status='QUEUED'))

        job = self.repo.get(j)
        self.assertEqual(2, self.repo.count_jobs_before(job))

    def test_get_next(self):
        first = self.repo.save(JobDTO(study='111', version='1.1',  status='QUEUED'))
        sleep(0.1)
        self.repo.save(JobDTO(study='222', version='1.1',  status='COMPUTING'))
        sleep(0.1)
        second = self.repo.save(JobDTO(study='333', version='1.1',  status='QUEUED'))
        sleep(0.1)
        self.repo.save(JobDTO(study='444', version='1.1',  status='TERMINATED'))

        job = self.repo.get_next(version='1.1')
        self.assertEqual(first, job.id)

        job.status = 'TERMINATED'
        self.repo.save(job)

        job = self.repo.get_next(version='1.1')
        self.assertEqual(second, job.id)
