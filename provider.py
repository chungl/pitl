from abc import abstractmethod
import sqlite3
import os
from flask import Flask, request
from datetime import datetime, timedelta

datadir = os.environ.get('DATA_DIR', '/home/casey/pypi/')
file = 'weights.db'
table = 'measurements'

class Provider:
    @abstractmethod
    def get(self, from_ts=None, to_ts=None):
        pass

class SQLiteStore(Provider):
    def __init__(self, datadir, file, table):
        self.table = table
        self.ts_col = 'ts'

    def get(self, from_ts=None, to_ts=None):
        con=sqlite3.connect(os.path.join(datadir, file), check_same_thread=False)
        cur=con.cursor()
        conditions = []
        conditions_data = []
        if from_ts is not None:
            conditions.append(f'ts > ?')
            conditions_data.append(from_ts)
        if to_ts is not None:
            conditions.append(f'ts <= {to_ts}')
            conditions_data.append(to_ts)
        conditions_str = f" WHERE {' AND '.join(conditions)}" if len(conditions) else ''
        query = f'SELECT * FROM {self.table}{conditions_str};'
        data = cur.execute(query, conditions_data).fetchall()
        con.close()
        return data

print("Hello, world")
app = Flask(__name__)

provider = SQLiteStore(datadir, file, table)

@app.route("/")
def get_data():
    from_ts = request.params.get('from_ts')
    to_ts = request.params.get('to_ts')
    print(f'from_ts {from_ts}, to_ts {to_ts}')

    return provider.get(from_ts=datetime.now() - timedelta(minutes=5))