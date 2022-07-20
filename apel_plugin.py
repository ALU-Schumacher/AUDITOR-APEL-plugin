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
import configparser


async def get_records(client):
    response = await client.get()
    return response


async def get_records_since(client, start_time):
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


async def get_start_time(config):
    time_db_path = config['paths']['time_db_path']

    try:
        conn = sqlite3.connect(time_db_path)
        cur = conn.cursor()
        cur.row_factory = lambda cursor, row: row[0]
        cur.execute('SELECT last_end_time FROM times')
        start_time = cur.fetchall()[0]
        cur.close()
        conn.close()
        return start_time
    except Error as e:
        print(e)


async def get_report_time(config):
    time_db_path = config['paths']['time_db_path']

    if Path(time_db_path).is_file():
        try:
            conn = sqlite3.connect(time_db_path)
            cur = conn.cursor()
            cur.row_factory = lambda cursor, row: row[0]
            cur.execute('SELECT last_report_time FROM times')
            report_time = cur.fetchall()[0]
            cur.close()
            conn.close()
            return report_time
        except Error as e:
            print(e)
    else:
        report_time = await create_time_db(time_db_path)
        return report_time


async def update_time_db(config, stop_time, report_time):
    time_db_path = config['paths']['time_db_path']

    update_sql = ''' UPDATE times
                     SET last_end_time = ? ,
                         last_report_time = ?'''
    try:
        conn = sqlite3.connect(time_db_path)
        cur = conn.cursor()
        cur.execute(update_sql, (stop_time, report_time))
        conn.commit()
        cur.close()
        conn.close()
    except Error as e:
        print(e)


async def create_report(config, records):
    site_name = config['site'].get('site_name', fallback=None)

    report = 'APEL-summary-job-message: v0.3\n'
    for r in records:
        print('try apel style')
        if site_name is not None:
            site_id = site_name
        else:
            site_id = r['site_id']
        user_id = r['user_id']
        report_part = f'''User: {user_id}
Site: {site_id}
%%\n'''
        report += report_part

    print(report)

    return 'nagut'


async def run(config, client):
    run_interval = config['intervals'].getint('run_interval')
    report_interval = config['intervals'].getint('report_interval')

    await client.start()

    while True:
        last_report_time = await get_report_time(config)
        last_report_time = datetime.strptime(last_report_time, '%Y-%m-%dT%H:%M:%SZ')
        current_time = datetime.now()

        print((current_time-last_report_time).total_seconds())

        if not (current_time-last_report_time).total_seconds() >= report_interval:
            print('Too soon, do nothing for now!')
            await asyncio.sleep(run_interval)
            continue
        else:
            print('Enough time passed since last report, do it again!')

        start_time = await get_start_time(config)

        records = await get_records_since(client, start_time)

        for r in records:
            print(r)

        try:
            latest_stop_time = records[-1]['stop_time']
            report = await create_report(config, records)
            # await send_report(report)
            latest_report_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            await update_time_db(config, latest_stop_time, latest_report_time)
            print(latest_stop_time)
        except IndexError:
            print('No new records, do nothing for now!')

        await asyncio.sleep(run_interval)


def main():
    logging.basicConfig(level=logging.DEBUG)

    config = configparser.ConfigParser()
    config.read('apel_plugin.cfg')

    auditor_ip = config['auditor']['auditor_ip']
    auditor_port = config['auditor'].getint('auditor_port')
    client = AuditorClient(auditor_ip, auditor_port, num_workers=1, db=None)

    try:
        asyncio.run(run(config, client))
    except KeyboardInterrupt:
        print("User abort")
    finally:
        asyncio.run(client.stop())


if __name__ == '__main__':
    main()
