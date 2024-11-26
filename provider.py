from abc import abstractmethod
import sqlite3
import os
from flask import Flask, request, send_file
from datetime import datetime, timedelta
import json

datadir = os.environ.get('DATA_DIR', '/home/casey/data/')
file = 'weights.db'
table = 'measurements'
clipdir = os.path.join(datadir, 'clips')

class Provider:
    @abstractmethod
    def get(self, from_ts=None, to_ts=None):
        pass

class SQLiteStore(Provider):
    def __init__(self, datadir, file, table):
        self.table = table
        self.ts_col = 'ts'

    def get(self, from_ts=None, to_ts=None, limit=None):
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
        limit_str = f" LIMIT {limit}" if limit is not None else ""
        conditions_str = f" WHERE {' AND '.join(conditions)}" if len(conditions) else ''
        query = f'SELECT * FROM {self.table}{conditions_str}{limit_str};'
        data = cur.execute(query, conditions_data).fetchall()
        con.close()
        return data

app = Flask(__name__)

provider = SQLiteStore(datadir, file, table)

@app.route("/")
def get_data():
    from_ts=None
    to_ts=None
    limit=10_000
    try:
        from_str = request.args.get('from_ts')
        if from_str is not None:
            from_ts = datetime.fromisoformat(from_str)
    except ValueError:
        return 'Invalid from_ts: Expected YYYY-MM-DD HH:MM:SS+', 400 
    
    try:  
        to_ts = request.args.get('to_ts')
    except ValueError:
        return 'Invalid to_ts: Expected YYYY-MM-DD HH:MM:SS+', 400
    
    try:
        limit_str = request.args.get('limit')
        if limit_str is not None:
            limit = int(limit_str)
    except ValueError:
        return 'Invalid limit: expected integer', 400
    
    print(f'from_ts {from_ts}, to_ts {to_ts}, limit {limit}')

    return json.dumps(provider.get(from_ts=from_ts, to_ts=to_ts,limit=limit))

def walk(path):
    for dirpath, dirs, filenames in os.walk(path):
        for f in filenames:
            yield dirpath, f


@app.route('/clips')
def get_file():
    from_name=request.args.get('from')
    
    # filename = (dirpath, filename)
    filenames = list(walk(clipdir))

    sorted_files = sorted(filenames, key=lambda t: t[1])
    print(f'sorted files {sorted_files}')
    if from_name:
        # Scan through sorted files and trim to the index of the first file newer than the filter
        # If no files are newer, create empty list by trimming to len(files)
        from_index = len(sorted_files)
        for i in range(len(sorted_files)):
            if sorted_files[i][1] > from_name:
                from_index = i
                break
        sorted_files = sorted_files[from_index:]

    print(f'get_file: from_ts {from_name}, num_files {len(sorted_files)}')

    if len(sorted_files) == 0:
        return 'No matching files', 204
    filepath = os.path.join(*sorted_files[0])
    print(f'Sending file {filepath} {sorted_files[0]}')
    return send_file(filepath, as_attachment=True)
