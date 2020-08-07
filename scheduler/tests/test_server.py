import os
import pickle
import unittest

from scheduler.server import application
from storage import Job, JobRepository


class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        application.config['TESTING'] = True
        application.config['WTF_CSRF_ENABLED'] = False
        application.config['DEBUG'] = False

        self.repo = JobRepository()
        self.app = application.test_client()

    def tearDown(self) -> None:
        os.remove('db.sqlite3')

    def test_send_study(self):
        res = self.app.post('/study', data=pickle.dumps('Hello Study'))
        res = pickle.loads(res.data)

        # Verify response
        self.assertIsNotNone(res['job'])
        self.assertEqual('QUEUED', res['status'])
        self.assertEqual(1, res['progress'])

        # Verify repo
        job = self.repo.get(res['job'])
        self.assertIsNotNone(job)
        self.assertEqual('Hello Study', job.study)

    def test_get_result_terminated(self):
        # Input
        job = Job(study='Hello world', id='123', created=147, status='TERMINATED', result='Bonjour le monde')
        self.repo.save(job)

        # Test & Verify
        res = self.app.get('/result/123')
        res = pickle.loads(res.data)

        self.assertEqual('TERMINATED', res['status'])
        self.assertEqual('Bonjour le monde', res['result'])

    def test_get_result_queued(self):
        # Input
        self.repo.save(Job(study='Hello world', id='456', created=100, status='QUEUED', result=''))
        self.repo.save(Job(study='Hello world', id='123', created=147, status='QUEUED', result=''))

        # Test & Verify
        res = self.app.get('/result/123')
        res = pickle.loads(res.data)

        self.assertEqual('QUEUED', res['status'])
        self.assertEqual(1, res['progress'])

    def test_get_next_job(self):
        # Input
        self.repo.save(Job(study='Hello world', id='123', created=147, status='QUEUED', result=''))

        # Test & Verify
        res = self.app.get('/job/next')
        job = pickle.loads(res.data)
        self.assertIsNotNone(job)
        self.assertIsNotNone(job.study)
        self.assertEqual('123', job.id)

    def test_update_job(self):
        # Input
        job = Job(study='Hello world', id='123', created=147, status='COMPUTING', result='Bonjour le monde')

        # Test & Verify
        self.app.post('/job/123', data=pickle.dumps(job))

        saved = self.repo.get('123')
        self.assertEqual('Bonjour le monde', saved.result)
        self.assertEqual('TERMINATED', saved.status)
