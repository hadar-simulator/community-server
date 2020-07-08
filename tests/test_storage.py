import unittest
import hadar as hd

from storage import JobRepository, Job


class TestJobRepository(unittest.TestCase):
    def test_db(self):
        repo = JobRepository(path='db.sqlite3')

        study = hd.Study(horizon=1)\
            .network().node('a').consumption(name='load', cost=10, quantity=10).build()
        job = Job(study)

        repo.save(job)

        job = repo.get(job.id)
        self.assertIsNotNone(job.id)
        self.assertEqual(study, job.study)
        self.assertEqual('QUEUED', job.status)
        self.assertIsNone(job.result)