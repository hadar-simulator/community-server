import pickle
import unittest
from time import sleep

from scheduler.storage import JobRepository, Job


class TestJobRepository(unittest.TestCase):

    def setUp(self) -> None:
        self.repo = JobRepository(path=':memory:')

    def test_db(self):
        job = Job(study=b'Hello, World', error='wrong')

        self.repo.save(job)

        job = self.repo.get(job.id)
        self.assertIsNotNone(job.id)
        self.assertTrue(job.created > 0)
        self.assertEqual(b'Hello, World', job.study)
        self.assertEqual('QUEUED', job.status)
        self.assertIsNone(job.result)
        self.assertEqual('wrong', job.error)

    def test_delete_terminated(self):
        t = self.repo.save(Job(study=b'aaa', status='TERMINATED'))
        e = self.repo.save(Job(study=b'bbb', status='ERROR'))
        q = self.repo.save(Job(study=b'ccc', status='QUEUED'))

        self.repo.delete_terminated()

        self.assertIsNone(self.repo.get(t))
        self.assertIsNone(self.repo.get(e))
        self.assertIsNotNone(self.repo.get(q))

    def test_count_jobs_before(self):
        self.repo.save(Job(study=b'111', status='QUEUED'))
        sleep(0.1)
        self.repo.save(Job(study=b'222', status='QUEUED'))
        sleep(0.1)
        j = self.repo.save(Job(study=b'333', status='QUEUED'))
        sleep(0.1)
        self.repo.save(Job(study=b'444', status='QUEUED'))

        job = self.repo.get(j)
        self.assertEqual(2, self.repo.count_jobs_before(job))

    def test_get_next(self):
        first = self.repo.save(Job(study=b'111', status='QUEUED'))
        sleep(0.1)
        self.repo.save(Job(study=b'222', status='COMPUTING'))
        sleep(0.1)
        second = self.repo.save(Job(study=b'333', status='QUEUED'))
        sleep(0.1)
        self.repo.save(Job(study=b'444', status='TERMINATED'))

        job = self.repo.get_next()
        self.assertEqual(first, job.id)

        job.status = 'TERMINATED'
        self.repo.save(job)

        job = self.repo.get_next()
        self.assertEqual(second, job.id)
