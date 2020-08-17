import os
import pickle
import shutil
import unittest
from time import sleep

from scheduler.storage import JobRepository, JobDTO


class TestJobRepository(unittest.TestCase):

    def setUp(self) -> None:
        self.repo = JobRepository(path='data')

    def tearDown(self) -> None:
        if os.path.exists('data'):
            shutil.rmtree('data')

    def test_db(self):
        job = JobDTO(study=b'Hello, World', version='1', error='wrong', result=b'Bonjour le Monde')

        self.repo.save(job)

        job = self.repo.get(job.id)
        self.assertIsNotNone(job.id)
        self.assertTrue(job.created > 0)
        self.assertEqual(b'Hello, World', job.study)
        self.assertEqual('QUEUED', job.status)
        self.assertEqual(b'Bonjour le Monde', job.result)
        self.assertEqual('wrong', job.error)
        self.assertTrue(os.path.exists('data/studies/%s' % job.id))
        self.assertTrue(os.path.exists('data/results/%s' % job.id))

    def test_delete_terminated(self):
        t = self.repo.save(JobDTO(study=b'aaa', version='1', status='TERMINATED'))
        e = self.repo.save(JobDTO(study=b'bbb', version='1', status='ERROR'))
        q = self.repo.save(JobDTO(study=b'ccc', version='1', status='QUEUED'))

        self.repo.delete_terminated()

        self.assertIsNone(self.repo.get(t))
        self.assertIsNone(self.repo.get(e))
        self.assertIsNotNone(self.repo.get(q))

    def test_count_jobs_before(self):
        self.repo.save(JobDTO(study=b'111', version='1', status='QUEUED'))
        sleep(0.1)
        self.repo.save(JobDTO(study=b'222', version='1',  status='QUEUED'))
        sleep(0.1)
        j = self.repo.save(JobDTO(study=b'333', version='1',  status='QUEUED'))
        sleep(0.1)
        self.repo.save(JobDTO(study=b'444', version='1',  status='QUEUED'))

        job = self.repo.get(j)
        self.assertEqual(2, self.repo.count_jobs_before(job))

    def test_get_next(self):
        first = self.repo.save(JobDTO(study=b'111', version='1',  status='QUEUED'))
        sleep(0.1)
        self.repo.save(JobDTO(study=b'222', version='1',  status='COMPUTING'))
        sleep(0.1)
        second = self.repo.save(JobDTO(study=b'333', version='1',  status='QUEUED'))
        sleep(0.1)
        self.repo.save(JobDTO(study=b'444', version='1',  status='TERMINATED'))

        job = self.repo.get_next()
        self.assertEqual(first, job.id)

        job.status = 'TERMINATED'
        self.repo.save(job)

        job = self.repo.get_next()
        self.assertEqual(second, job.id)
