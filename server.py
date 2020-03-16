import pickle
import sys

from flask import Flask, request, abort
from hadar.solver.study import solve

app = Flask(__name__)


@app.route("/", methods=['POST'])
def compute():
    if token is not None and request.args['token'] != token:
        abort(403, 'Wrong access token given')

    study = pickle.loads(request.data)
    res = solve(study)
    return pickle.dumps(res)


if __name__ == '__main__':
    token = sys.argv[0] if len(sys.argv) > 1 else None
    app.run(debug=False, host='0.0.0.0')
