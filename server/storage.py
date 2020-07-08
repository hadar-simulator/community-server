import hashlib
import os
import pickle
import sqlite3

import hadar as hd


def get_hash(array):
    m = hashlib.sha256()
    m.update(array)
    return m.hexdigest()


class Job:
    """
    Entity stored in db.
    """
    def __init__(self, study: hd.Study, id: str = None, status: str = 'QUEUED', result: hd.Result = None):
        self.study = study
        self.status = status
        self.id = get_hash(pickle.dumps(study)) if id is None else id
        self.result = result

    def __reduce__(self):
        return self.id, pickle.dumps(self.study), self.status, pickle.dumps(self.result)


class JobRepository:
    """
    Repository to manage sqlite3 database
    """

    def __init__(self, path: str = None):
        """
        Init database, create job table if not exist.

        :param path: sqlite3 path to database
        """
        path = path or os.getenv('DB_PATH', 'db.sqlite3')
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()
        self.cur.execute("create table if not exists job (id TEXT PRIMARY KEY, study BLOB, status TEXT, result BLOB)")
        self.conn.commit()

    def save(self, job: Job):
        """
        Save job, create if not exist update else.

        :param job: job to save
        :return:
        """
        data = job.__reduce__()
        data = data + data[2:]
        self.cur.execute("""INSERT INTO job (id, study, status, result) VALUES (?, ?, ?, ?)
                            ON CONFLICT(id) 
                            DO UPDATE SET status=?, result=?;""", data)
        self.conn.commit()
        return job.id

    def get(self, job_id: str):
        """
        Get job by id.

        :param job_id: job id
        :return: job with matching id
        """
        res = self.cur.execute("SELECT * FROM job WHERE id = ?", (job_id,)).fetchone()
        if res:
            job_id, study, status, result = res
            return Job(id=job_id, study=pickle.loads(study), status=status, result=pickle.loads(result))
        else:
            return Job(study=None)

    def delete_terminated(self):
        """
        Delete job terminated
        :return:
        """
        self.cur.execute("DELETE FROM job WHERE status = 'TERMINATED'")
        self.conn.commit()
