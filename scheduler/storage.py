import hashlib
import os
import sqlite3
from threading import Lock
import time


lock = Lock()


def sha256(array):
    m = hashlib.sha256()
    m.update(array)
    return m.hexdigest()


class JobEntity:
    """
    Entity stored in db.
    """
    def __init__(self,
                 id: str,
                 version: str,
                 created: int = 0,
                 computed: int = 0,
                 terminated: int = 0,
                 status: str = 'QUEUED',
                 error: str = ''):
        self.created = created or int(time.time() * 1000)
        self.computed = computed
        self.terminated = terminated
        self.status = status
        self.id = id
        self.version = version
        self.error = error


class JobDTO(JobEntity):
    """
    Entity stored in db.
    """
    def __init__(self, study: bytes,
                 version: str,
                 created: int = 0,
                 computed: int = 0,
                 terminated: int = 0,
                 id: str = None,
                 status: str = 'QUEUED',
                 result: bytes = None,
                 error: str = ''):
        super().__init__(id, version, created, computed, terminated, status, error)
        self.study = study
        self.result = result
        self.id = id or sha256(study)


class JobRepository:
    """
    Repository to manage sqlite3 database
    """

    def __init__(self, path: str = None):
        """
        Init database, create job table if not exist.

        :param path: sqlite3 path to database
        """
        path = path or os.getenv('DATA_PATH', 'data')
        self.studies_dir = '%s/studies/' % path
        self.results_dir = '%s/results/' % path

        os.makedirs(self.studies_dir, exist_ok=True)
        os.makedirs(self.results_dir, exist_ok=True)

        self.conn = sqlite3.connect('%s/db.sqlite3' % path)
        self.cur = self.conn.cursor()

        lock.acquire()
        self.cur.execute("""create table if not exists job (
                            id TEXT PRIMARY KEY,
                            version TEXT,
                            created NUMERIC, 
                            computed NUMERIC,
                            terminated NUMERIC,
                            status TEXT,
                            error TEXT)""")
        self.conn.commit()

        # Move all study from computing to queued
        self.cur.execute("""UPDATE job SET status = 'QUEUED'
                            WHERE status = 'COMPUTING'""")
        self.conn.commit()
        lock.release()

    def save(self, job: JobDTO):
        """
        Save job, create if not exist update else.

        :param job: job to save
        :return:
        """
        exist = self.get(job.id) is not None

        lock.acquire()
        # Save in DB
        if exist:
            self.cur.execute("UPDATE job SET computed = ?, terminated = ?, status = ?, error = ? WHERE id = ?",
                             (job.computed, job.terminated, job.status, job.error, job.id))
        else:
            self.cur.execute("INSERT INTO job (id, version, created, computed, terminated, status, error)"
                             "VALUES (?, ?, ?, ?, ?, ?, ?);",
                             (job.id, job.version, job.created, job.computed, job.terminated, job.status, job.error))
        self.conn.commit()

        # Save on disk
        with open(self.studies_dir + job.id, 'wb') as f:
            f.write(job.study)

        with open(self.results_dir + job.id, 'wb') as f:
            f.write(job.result or b'')

        lock.release()
        return job.id

    def _map_job(self, res):
        """
        Map job from database row to JobDTO object
        :param res: database row
        :return: Job DTO
        """
        job_id, version, created, computed, terminated, status, error = res

        with open(self.studies_dir + job_id, 'rb') as f:
            study = f.read()
        with open(self.results_dir + job_id, 'rb') as f:
            result = f.read()

        return JobDTO(id=job_id, version=version, study=study, created=created, computed=computed,
                      terminated=terminated, status=status, result=result, error=error)


    def get(self, job_id: str):
        """
        Get job by id.

        :param job_id: job id
        :return: job with matching id
        """
        lock.acquire()
        res = self.cur.execute("SELECT * FROM job WHERE id = ?", (job_id,)).fetchone()
        lock.release()
        return self._map_job(res) if res else None

    def delete_terminated(self):
        """
        Delete job terminated.

        :return:
        """
        lock.acquire()
        ids = self.cur.execute("SELECT id FROM job WHERE (status = 'TERMINATED' OR status = 'ERROR')")
        for (i, ) in tuple(ids):
            os.remove(self.studies_dir + i)
            os.remove(self.results_dir + i)

            self.cur.execute("DELETE FROM job WHERE id = ?", (i,))
            self.conn.commit()
        lock.release()

    def count_jobs_before(self, job: JobDTO):
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
        return self._map_job(res) if res else None
