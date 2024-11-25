from abc import abstractmethod
import sqlite3
import os
from typing import Any
import urllib.parse
import re
import math

import requests
import matplotlib
import pandas as pd
from datetime import datetime, timedelta
from scipy import stats
import subprocess


def walk(path):
    for dirpath, dirs, filenames in os.walk(path):
        for f in filenames:
            yield dirpath, f

class Provider:
    @abstractmethod
    def get(self, from_ts=None, to_ts=None):
        pass

class SQLiteStore(Provider):
    def __init__(self, datadir, file, table, host, scale_factor=1, offset=0, plotcolor=None):
        self.datadir = datadir
        self.file = file
        self.table = table
        self.ts_col = 'ts'
        self.ts_strfmt = '%Y-%m-%d %X'

        if math.isnan(scale_factor) or scale_factor == 0:
            raise ValueError("Provider: Invalid scale_factor. Must be non-zero number.")
        self.scale_factor = scale_factor
        if math.isnan(offset):
            raise ValueError('Provider: Invalid offset. Must be number.')
        self.offset = offset

        self.host = host

        self.plotcolor=plotcolor

        self.con=self.connection()
        self.migrate()
    
    @property
    def cursor(self):
        return self.con.cursor()

    def get(self, from_ts=None, to_ts=None):
        conditions = []
        conditions_data = []
        if from_ts is not None:
            conditions.append(f'{self.ts_col} >= ?')
            conditions_data.append(from_ts)
        if to_ts is not None:
            conditions.append(f'{self.ts_col} < {to_ts}')
            conditions_data.append(to_ts)
        conditions_str = f" WHERE {' AND '.join(conditions)}" if len(conditions) else ''
        query = f'SELECT * FROM {self.table}{conditions_str};'
        cur = self.cursor
        cur.execute(query, conditions_data).fetchall()
        cur.close()

    def writeall(self, data):
        con = self.connection(timeout=20)
        query = f"INSERT INTO {self.table} VALUES ({','.join('?'*len(data[0]))})"
        con.cursor().executemany(query, data)
        con.commit()
        con.close()

    def getremote(self, url, from_ts=None, to_ts=None):
        data = None
        print(data)
        return data

    def strftime(self, dt):
        return dt.strftime('%Y-%m-%d %X')
        
    def todf(self, xmin=None, xmax=None, xrange=None, query=None, filter=True):
        query_conditions=[]
        if xrange is not None:
            if xmin is None and xmax is not None:
                xmin = xmax - xrange
            elif xmax is None and xmin is not None:
                xmax = xmin + xrange
        if xmin is not None:
            query_conditions.append(f'{self.ts_col} >= "{xmin.strftime(self.ts_strfmt)}"')
        if xmax is not None:
            query_conditions.append(f'{self.ts_col} <= "{xmax.strftime(self.ts_strfmt)}"')
        if query is None:
            conditions_str = f' WHERE {" AND ".join(query_conditions)}' if len(query_conditions) else ""
            query = f'SELECT * FROM {self.table}{conditions_str}'
        df = pd.read_sql_query(query, self.con)
        if filter:
            df = self.filter_df(df)
        df['g'] = df['raw'].map(lambda y: int((y-self.offset)/self.scale_factor))
        df['kg'] = df['g'].map(lambda y: y/1000)
        df['lb'] = df['g'].map(lambda y: y/453.592)
        df['x'] = df['ts'].map(lambda x: datetime.fromisoformat(x))
        df['lbmed5k'] = df['lb'].rolling(window=5000).median()
        df['lbmed100'] = df['lb'].rolling(window=100).median()
        df['catlb'] = df['lbmed100'] - df['lbmed5k']
        return df
    
    def catchup(self, limit=None):
        cur = self.cursor
        ts, = cur.execute(f'SELECT {self.ts_col} FROM {self.table} ORDER BY {self.ts_col} DESC').fetchone()
        cur.close()
        print(f'Updating {self.table} from {ts}')
        params = {'from_ts': ts}
        if limit is not None:
            params['limit'] = limit
        
        response = requests.get(f'{self.host}?{urllib.parse.urlencode(params)}')
        response.raise_for_status()
        newdata = response.json()
        if len(newdata) == 0:
            print('Received no data')
            return
        self.writeall(newdata)
        print(f'Added {len(newdata)} rows')

#     @app.route('/clips')
# def get_file():
#     from_name=request.args.get('from')
    
#     # filename = (dirpath, filename)
#     filenames = list(walk(clipdir))

#     sorted_files = sorted(filenames, key=lambda t: t[1])
#     print(f'sorted files {sorted_files}')
#     if from_name:
#         # Scan through sorted files and trim to the index of the first file newer than the filter
#         # If no files are newer, create empty list by trimming to len(files)
#         from_index = len(sorted_files)
#         for i in range(len(sorted_files)):
#             if sorted_files[i][1] > from_name:
#                 from_index = i
#                 break
#         sorted_files = sorted_files[from_index:]

#     print(f'get_file: from_ts {from_name}, num_files {len(sorted_files)}')

#     if len(sorted_files) == 0:
#         return 'No matching files', 204
#     filepath = os.path.join(*sorted_files[0])
#     print(f'Sending file {filepath} {sorted_files[0]}')
#     return send_file(filepath, as_attachment=True)

    def convert_file(self, filepath, fps=3):
        subprocess.run(['ffmpeg', '-i', filepath, '-vf' ,f"setpts={30/fps}*PTS",filepath.replace('.h264', '.mp4')])


    def catchup_files(self, host, dirpath, recursion_max=None, fps=30):
        filenames = list(walk(dirpath))
        sorted_files = sorted(filenames, key=lambda t: t[1])
        params = {}
        if len(sorted_files):
            params['from'] = sorted_files[-1][1]

        response = requests.get(f'{host}?{urllib.parse.urlencode(params)}', stream=True)
        response.raise_for_status()
        if response.status_code == 204:
            print('No new files')
            return
        try:
            filename, = re.search(r'filename *= *(.+)$', response.headers['Content-Disposition']).groups()
        except (ValueError, AttributeError):
            print(f'Error: catchup received file without name {response.headers}')
            return
        try:
            with open(os.path.join(dirpath, filename),'xb') as destination:
                for chunk in response.iter_content():
                    destination.write(chunk)
            self.convert_file(os.path.join(dirpath, filename), fps)
        except FileExistsError:
            print(f"Warning: Skipping existing file {filename}")
            return
        print(f'Received file {filename}')

        if recursion_max is None or recursion_max > 0:
            return self.catchup_files(host, dirpath, recursion_max=recursion_max-1 if recursion_max is not None else None)
    
    def connection(self, timeout=None):
        connection_args = {}
        if timeout is not None:
            connection_args['timeout'] = timeout
        path_to_db = os.path.join(self.datadir, self.file)
        print(f'Will connect to {path_to_db}')
        return sqlite3.connect(path_to_db, **connection_args)
    
    def migrate(self):
        cur = self.cursor
        cur.execute("CREATE TABLE IF NOT EXISTS measurements (id INTEGER PRIMARY KEY AUTOINCREMENT, ts, raw, config_id)")
        cur.execute("CREATE TABLE IF NOT EXISTS configs (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, g_factor, raw_offset)")
        cur.close()


    def filter_df(self, df):
        filtered = df[~df.index.isin(df.index[df['raw']==0])].copy()
        filtered['raw'] = filtered['raw'].rolling(window=3).median()
        filtered = filtered[~filtered['raw'].isna()].copy()
        return filtered


    def line(self, yseries, x0=None, y0=None, xrange=None, yrange=None, yfilter=None, xseries='x', df=None, **plot_args):
        if x0 is not None:
            if xrange is not None:
                plot_args['xlim'] = (x0, x0 + xrange)

        df = df if df is not None else self.todf(xmin=x0, xrange=xrange)

        if yfilter is not None:
            if yfilter is True and y0 is not None and yrange is not None:
                yfilter = (y0, y0 + yrange)
            ymin, ymax = yfilter
            df = df[(df[yseries]>ymin) & (df[yseries]<ymax)]

        if y0 is not None and yrange is not None:
            plot_args['ylim'] = (y0, y0 + yrange)
        
        if self.plotcolor and not plot_args.get('color'):
            plot_args['color'] = self.plotcolor

        df.plot.line(xseries, yseries, **plot_args)
        matplotlib.pyplot.show()


if __name__ == '__main__':
    store = SQLiteStore('/Users/casey/data/cats/','weights.db','measurements', 'http://cats.local:8000', scale_factor=-10.97, offset = -35800)
    store2 = SQLiteStore('/Users/casey/data/cats/','weights2.db','measurements', 'http://cam.local:8000', scale_factor=10.97, offset=-145700, plotcolor="red")
    
