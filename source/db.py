import sqlite3
import os

datadir = os.environ.get('DATA_DIR', '/home/casey/data/')

con=sqlite3.connect(os.path.join(datadir, 'weights.db'))
cur=con.cursor()

def migrate():
    cur.execute("CREATE TABLE IF NOT EXISTS measurements (id INTEGER PRIMARY KEY AUTOINCREMENT, ts, raw, config_id)")
    cur.execute("CREATE TABLE IF NOT EXISTS configs (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, g_factor, raw_offset)")

def count():
    return cur.execute('SELECT count(*) FROM measurements').fetchone()

migrate()
