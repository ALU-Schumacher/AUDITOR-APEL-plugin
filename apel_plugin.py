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
    response = await client.get_stopped_since(start_time)
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
    conn = sqlite3.connect(time_db_path)
    cur = conn.cursor()
    cur.row_factory = lambda cursor, row: row[0]
    cur.execute('SELECT last_end_time FROM times')
    start_time = cur.fetchall()[0]
    cur.close()
    conn.close()

    return start_time

async def get_report_time(time_db_path):
    if Path(time_db_path).is_file():
        conn = sqlite3.connect(time_db_path)
        cur = conn.cursor()
        cur.row_factory = lambda cursor, row: row[0]
        cur.execute('SELECT last_report_time FROM times')
        report_time = cur.fetchall()[0]
        cur.close()
        conn.close()
    else:
        report_time = await create_time_db(time_db_path)

    return report_time

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

async def main(client: AuditorClient, time_db_path, run_interval, report_interval):
    while True:
        last_report_time = await get_report_time(time_db_path)
        last_report_time = datetime.strptime(last_report_time,'%Y-%m-%dT%H:%M:%SZ')
        current_time = datetime.now()

        print((current_time-last_report_time).total_seconds())

        if not (current_time-last_report_time).total_seconds() >= report_interval:
            print('Too soon, do nothing for now!')
            await asyncio.sleep(run_interval)
            continue
        else:
            print('More than 1 minute since last report, do it again!')

        await client.start()

        start_time = await get_start_time(time_db_path)
        #    print(start_time)
        records = await get_records_since(start_time)
        for r in records:
            print(r)
        try:
            latest_stop_time = records[-1]['stop_time']
            # report = await create_report(records)
            # await send_report()
            print(latest_stop_time)
        except:
            latest_stop_time = start_time
        latest_report_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        await update_time_db(latest_stop_time, latest_report_time, time_db_path)

        await client.stop()
        await asyncio.sleep(run_interval)

if __name__ == '__main__':

    client = AuditorClient('10.18.1.64', 8000, num_workers=1, db=None)
    time_db_path = '/work/ws/atlas/ds1034-output/time.db'
    run_interval = 5
    report_interval = 60

    logging.basicConfig(level=logging.DEBUG)

    try:
        asyncio.run(main(client,time_db_path,run_interval,report_interval))
    except KeyboardInterrupt:
        print("User abort")
        pass
    finally:
        asyncio.run(client.stop())
