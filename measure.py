#!/usr/bin/python3
from hx711 import HX711
from RPi import GPIO
import logging
from datetime import datetime, timedelta
from sqlite3 import OperationalError
import requests
import os

print('Measure: Initializing...')
from db import con, cur

logger = logging.getLogger(__name__)


class HX(HX711):
    def readstream(self):
        while True:
            try:
                yield (datetime.now(), self._read())
            except Exception as e:
                logger.error(f'Failed to read: {e}')    

g_factor = -10.97
g_lb = 454
offset = -35800
DEBUG = os.environ.get('DEBUG', False)

config = cur.execute("SELECT id, g_factor, raw_offset FROM configs ORDER BY created_at DESC LIMIT 1").fetchone()
if config is None or config[1] != g_factor or config[2] != offset:
    cur.execute("INSERT INTO configs (g_factor, raw_offset) VALUES (?,?))",(g_factor, offset))
    con.commit()
    config =cur.execute("SELECT id FROM configs ORDER BY created_at DESC LIMIT 1").fetchone()
config_id = config[0]

recording=False
threshold_time=datetime.now()
try:
    hx711 = HX(
        dout_pin=22,
        pd_sck_pin=27,
        channel='A',
        gain=64
    )

    hx711.reset()   # Before we start, reset the HX711 (not obligate)
    print('Measure: Collecting data...')
    for ts, data in hx711.readstream():
        try:
            g=int((data-offset)/g_factor)
            p=float(int(g*100/454)/100)
            if DEBUG:
                print(f"{ts} {data:12,} long; {g:12,}g; {p:3} pounds")
            cur.execute(f"INSERT INTO measurements (ts, raw, config_id) VALUES (?,?,?)", (ts, data, config_id))
            con.commit()
            if p > 12 and p < 100:
                if not recording:
                    if DEBUG:
                        print(f'{datetime.now()} Starting recording: {p} lbs')
                    recording = True
                    requests.get('http://localhost:9000/start')
                threshold_time=datetime.now()
            elif recording:
                if (datetime.now()-threshold_time)>timedelta(seconds=15):
                    if DEBUG:
                        print(f'{datetime.now()} Stopping recording: {p} lbs')
                    requests.get('http://localhost:9000/stop')
                    recording = False

        except OperationalError as e:
            print(f"WARNING: Failed to insert datapoint {ts}, {data}: {e}")
        except Exception as e:
            print(f"ERROR: Unknown exception for datapoint {ts}, {data}: {e}")
except KeyboardInterrupt:
    pass
finally:
    print('Measure: Shutting down...')
    GPIO.cleanup()  # always do a GPIO cleanup in your scripts!
    con.close()
    print('Measure: Stopped.')
