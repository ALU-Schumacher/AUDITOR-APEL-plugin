#!/usr/bin/env python3

from __future__ import annotations
import logging
import asyncio
from auditorclient.client import AuditorClient
from pprint import pprint
from pathlib import Path
import sqlite3
from sqlite3 import Error
from datetime import datetime

async def get_records():
    response = await client.get()
    return response

async def get_records_since(start_time):
    response = await client.get_started_since(start_time)
    return response

async def create_time_db(time_db_path):
    create_table_sql = ''' CREATE TABLE IF NOT EXISTS times (
                             target TEXT UNIQUE NOT NULL,
                             last_end_time TEXT NOT NULL,
                             last_report_time TEXT NOT NULL
                           ); '''

    insert_sql = ''' INSERT INTO times(target, last_end_time, last_report_time)
                     VALUES("arex/jura/apel:EGI","1970-01-01T01:00:00Z","1970-01-01T01:00:00Z")'''
    try:
        conn = sqlite3.connect(time_db_path)
        cur = conn.cursor()
        cur.execute(create_table_sql)
        cur.execute(insert_sql)
        conn.commit()
        cur.close()
        conn.close()
        return '1970-01-01T01:00:00Z'
    except Error as e:
        print(e)

async def get_start_time(time_db_path):
    if Path(time_db_path).is_file():
        conn = sqlite3.connect(time_db_path)
        cur = conn.cursor()
        cur.row_factory = lambda cursor, row: row[0]
        cur.execute('SELECT last_end_time FROM times')
        start_time = cur.fetchall()[0]
        cur.close()
        conn.close()
    else:
        start_time = await create_time_db(time_db_path)

    return start_time

async def update_time_db(stop_time, report_time, time_db_path):
    update_sql = ''' UPDATE times
                     SET last_end_time = ? ,
                         last_report_time = ?'''

    conn = sqlite3.connect(time_db_path)
    cur = conn.cursor()
    cur.execute(update_sql, (stop_time, report_time))
    conn.commit()
    cur.close()
    conn.close()

async def main(client: AuditorClient, time_db_path):
    await client.start()

    start_time = await get_start_time(time_db_path)
    print(start_time)
    records = await get_records_since(start_time)
    try:
        latest_stop_time = records[-1]['stop_time']
    except:
        latest_stop_time = start_time
        print(len(records))
    #await send_report()
    latest_report_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(latest_report_time)
    await update_time_db(latest_stop_time, latest_report_time, time_db_path)

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    
    loop = asyncio.get_event_loop()

    client = AuditorClient('10.18.1.64', 8000, num_workers=1, db=None)
    time_db_path = '/work/ws/atlas/ds1034-output/time.db'

    try:
        loop.run_until_complete(main(client,time_db_path))
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(client.stop())
    loop.close()

