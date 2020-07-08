import hashlib
import os
import pickle
import sqlite3
import threading

import hadar as hd


def get_hash(array):
    m = hashlib.sha256()
    m.update(array)
    return m.hexdigest()


class Singleton(type):
    """
    Singleton metaclass used by repository
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


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


class JobRepository(metaclass=Singleton):
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
        self.lock = threading.Lock()

    def save(self, job: Job):
        """
        Save job, create if not exist update else.

        :param job: job to save
        :return:
        """
        self.lock.acquire()
        data = job.__reduce__()
        data = data + data[2:]
        self.cur.execute("""INSERT INTO job (id, study, status, result) VALUES (?, ?, ?, ?)
                            ON CONFLICT(id) 
                            DO UPDATE SET status=?, result=?;""", data)
        self.conn.commit()
        self.lock.release()

    def get(self, id: str):
        """
        Get job by id.

        :param id: job id
        :return: job with matching id
        """
        self.lock.acquire()
        id, study, status, result = self.cur.execute("SELECT * FROM job WHERE id = ?", (id,)).fetchone()
        self.lock.release()
        return Job(id=id, study=pickle.loads(study), status=status, result=pickle.loads(result))
