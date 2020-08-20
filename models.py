import hashlib
import json
import time


def sha256(obj):
    m = hashlib.sha256()
    m.update(json.dumps(obj).encode())
    return m.hexdigest()

class JobDTO:
    """
    Entity stored in db.
    """
    def __init__(self, study: dict(),
                 version: str,
                 created: int = 0,
                 computed: int = 0,
                 terminated: int = 0,
                 id: str = None,
                 status: str = 'QUEUED',
                 result: dict() = None,
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

    def to_json(self):
        return self.__dict__

    @staticmethod
    def from_json(dict):
        return JobDTO(**dict)
