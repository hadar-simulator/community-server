import hashlib
import time


def sha256(array):
    m = hashlib.sha256()
    m.update(array)
    return m.hexdigest()

class JobDTO:
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
        self.created = created or int(time.time() * 1000)
        self.computed = computed
        self.terminated = terminated
        self.status = status
        self.id = id
        self.version = version
        self.error = error
        self.study = study
        self.result = result
        self.id = id or sha256(study)