import os
import json
import shutil
import unittest

from scheduler.server import application
from storage import JobDTO, JobRepository


class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        application.config['TESTING'] = True
        application.config['WTF_CSRF_ENABLED'] = False
        application.config['DEBUG'] = False

        self.repo = JobRepository()
        self.app = application.test_client()

    def tearDown(self) -> None:
        if os.path.exists('data'):
            shutil.rmtree('data')

    def test_send_study(self):
        res = self.app.post('/api/v2/study', data=json.dumps({'version': '1'}))
        res = json.loads(res.data)

        # Verify response
        self.assertIsNotNone(res['job'])
        self.assertEqual('QUEUED', res['status'])
        self.assertEqual(1, res['progress'])

        # Verify repo
        job = self.repo.get(res['job'])
        self.assertIsNotNone(job)
        self.assertEqual({'version': '1'}, job.study)

    def test_get_result_terminated(self):
        # Input
        job = JobDTO(study='Hello world', id='123', version='1', created=147, status='TERMINATED', result='Bonjour le monde')
        self.repo.save(job)

        # Test & Verify
        res = self.app.get('/api/v2/result/123')
        res = json.loads(res.data)

        self.assertEqual('TERMINATED', res['status'])
        self.assertEqual('Bonjour le monde', res['result'])

    def test_get_result_queued(self):
        # Input
        self.repo.save(JobDTO(study='Hello world', id='456', version='1',  created=100, status='QUEUED', result=''))
        self.repo.save(JobDTO(study='Hello world', id='123', version='1',  created=147, status='QUEUED', result=''))

        # Test & Verify
        res = self.app.get('/api/v2/result/123')
        res = json.loads(res.data)

        self.assertEqual('QUEUED', res['status'])
        self.assertEqual(1, res['progress'])

    def test_get_next_job(self):
        # Input
        self.repo.save(JobDTO(study='Hello world', id='123', version='1.1',  created=147, status='QUEUED', result=''))

        # Test & Verify
        res = self.app.get('/api/v2/job/next/1.1')
        job = JobDTO.from_json(json.loads(res.data))
        self.assertIsNotNone(job)
        self.assertIsNotNone(job.study)
        self.assertEqual('123', job.id)

    def test_update_job(self):
        # Input
        job = JobDTO(study='Hello world', id='123', created=147, version='1',  status='COMPUTING', result='Bonjour le monde')

        # Test & Verify
        self.app.post('/api/v2/job/123', data=json.dumps(job.to_json()))

        saved = self.repo.get('123')
        self.assertEqual('Bonjour le monde', saved.result)
        self.assertEqual('TERMINATED', saved.status)
