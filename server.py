import os
import pickle

from flask import Flask, request, abort
from hadar.solver.study import solve

application = Flask(__name__)


@application.route("/", methods=['POST'])
def compute():
    token = os.environ.get('ACCESS_TOKEN', None)
    if token is not None and request.args['token'] != token:
        abort(403, 'Wrong access token given')

    print('Receive study...', end=' ')
    study = pickle.loads(request.data)
    res = solve(study)

    print('return result')
    return pickle.dumps(res)


if __name__ == '__main__':
    application.run(debug=False, host='0.0.0.0')
