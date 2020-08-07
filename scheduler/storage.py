import hashlib
import os
import pickle
import sqlite3
import threading
import time

import hadar as hd


lock = threading.Lock()


def sha256(array):
    m = hashlib.sha256()
    m.update(array)
    return m.hexdigest()


class Job:
    """
    Entity stored in db.
    """
    def __init__(self, study: hd.Study,
                 id: str = None,
                 created: int = None,
                 status: str = 'QUEUED',
                 result: hd.Result = None,
                 error: str = ''):
        self.study = study
        self.created = int(time.time() * 1000) if created is None else created
        self.status = status
        self.id = sha256(pickle.dumps(study)) if id is None else id
        self.result = result
        self.error = error

    def flatten(self):
        return self.id, pickle.dumps(self.study), self.created, self.status, pickle.dumps(self.result), self.error


class JobRepository:
    """
    Repository to manage sqlite3 database
    """

    def __init__(self, path: str = None):
        """
        Init database, create job table if not exist.

        :param path: sqlite3 path to database
        """
        path = path or os.getenv('DB_PATH', ':memory:')

        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()

        lock.acquire()
        self.cur.execute("""create table if not exists job (
                            id TEXT PRIMARY KEY,
                            study BLOB,
                            created NUMERIC,
                            status TEXT, 
                            result BLOB,
                            error TEXT)""")
        self.conn.commit()

        # Remove all study from computing to queued
        self.cur.execute("""UPDATE job SET status = 'QUEUED'
                            WHERE status = 'COMPUTING'""")
        self.conn.commit()
        lock.release()

    def save(self, job: Job):
        """
        Save job, create if not exist update else.

        :param job: job to save
        :return:
        """
        data = job.flatten()
        exist = self.get(job.id) is not None

        lock.acquire()
        if exist:
            self.cur.execute("UPDATE job SET status = ?, result = ?, error = ? WHERE id = ?", data[3:] + (job.id,))
        else:
            self.cur.execute("INSERT INTO job (id, study, created, status, result, error) VALUES (?, ?, ?, ?, ?, ?);", data)
        self.conn.commit()

        lock.release()
        return job.id

    def get(self, job_id: str):
        """
        Get job by id.

        :param job_id: job id
        :return: job with matching id
        """
        lock.acquire()
        res = self.cur.execute("SELECT * FROM job WHERE id = ?", (job_id,)).fetchone()
        lock.release()
        if res:
            job_id, study, created, status, result, error = res
            return Job(id=job_id, study=pickle.loads(study), created=created, status=status, result=pickle.loads(result), error=error)
        else:
            return None

    def delete_terminated(self):
        """
        Delete job terminated.

        :return:
        """
        lock.acquire()
        self.cur.execute("DELETE FROM job WHERE (status = 'TERMINATED' OR status = 'ERROR')")
        self.conn.commit()
        lock.release()

    def count_jobs_before(self, job: Job):
        """
        Get number of jobs to be computed before this one.

        :param job: job given
        :return: number of jobs before
        """
        lock.acquire()
        counting = self.cur.execute("SELECT COUNT(*) FROM job WHERE (status = 'QUEUED' AND created < ?)", (job.created,))\
            .fetchone()[0]
        lock.release()
        return counting

    def get_next(self):
        lock.acquire()
        res = self.cur.execute("""SELECT * FROM job
                                  WHERE created = (SELECT MIN(created) FROM job WHERE status = 'QUEUED');""").fetchone()
        lock.release()
        if res:
            job_id, study, created, status, result, error = res
            return Job(id=job_id, study=pickle.loads(study), created=created, status=status, result=pickle.loads(result), error=error)
        else:
            return None
