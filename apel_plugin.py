#!/usr/bin/env python3

from __future__ import annotations
import logging
import asyncio
from pyauditor import AuditorClientBuilder
from pprint import pprint
from pathlib import Path
import sqlite3
from sqlite3 import Error
from datetime import datetime
import configparser
import pytz


async def get_records(client):
    response = await client.get()
    return response


async def get_records_since(client, start_time):
    response = await client.get_stopped_since(start_time)
    return response


async def create_time_db(time_db_path):
    create_table_sql = ''' CREATE TABLE IF NOT EXISTS times (
                             target TEXT UNIQUE NOT NULL,
                             last_end_time timestamp NOT NULL,
                             last_report_time timestamp NOT NULL
                           ); '''

    insert_sql = ''' INSERT INTO times(target, last_end_time, last_report_time)
                     VALUES(?, ?, ?); '''
    ini_time = datetime(1970, 1, 1, 0, 0, 0)
    data_tuple = ('arex/jura/apel:EGI', ini_time, ini_time)

    try:
        conn = sqlite3.connect(time_db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cur = conn.cursor()
        cur.execute(create_table_sql)
        cur.execute(insert_sql, data_tuple)
        conn.commit()
        cur.close()
        conn.close()
        return ini_time
    except Error as e:
        print(e)


async def get_start_time(config):
    time_db_path = config['paths']['time_db_path']

    try:
        conn = sqlite3.connect(time_db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
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
            conn = sqlite3.connect(time_db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
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
                         last_report_time = ? '''
    try:
        conn = sqlite3.connect(time_db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cur = conn.cursor()
        cur.execute(update_sql, (stop_time, report_time))
        conn.commit()
        cur.close()
        conn.close()
    except Error as e:
        print(e)

# TODO: CREATE TABLE, FILL TABLE, MERGE RECORDS, CREATE SUMMARIES FROM MERGED RECORDS
async def create_records_db(records):
    # create_table_sql = ''' CREATE TABLE IF NOT EXISTS records (
    #                          year INTEGER NOT NULL,
    #                          month INTEGER NOT NULL,
    #                          user TEXT NOT NULL.
    #                          vo TEXT NOT NULL,
    #                          endpoint TEXT NOT NULL,
    #                          nodecount INTEGER NOT NULL,
    #                          cpucount INTEGER NOT NULL,
    #                          fqan TEXT NOT NULL,
    #                          benchmark TEXT NOT NULL,
    #                          recordid INTEGER NOT NULL,
    #                          walltime INTEGER NOT NULL,
    #                          cputime INTEGER NOT NULL,
    #                          starttime INTEGER NOT NULL,
    #                          endtime INTEGER NOT NULL
    #                        ); '''

    create_table_sql = ''' CREATE TABLE IF NOT EXISTS records (
                             year INTEGER NOT NULL,
                             month INTEGER NOT NULL,
                             user TEXT NOT NULL,
                             groupid TEXT NOT NULL,
                             cpucount INTEGER NOT NULL,
                             benchmarktype TEXT NOT NULL,
                             benchmarkvalue FLOAT NOT NULL,
                             recordid TEXT NOT NULL,
                             runtime INTEGER NOT NULL,
                             starttime INTEGER NOT NULL,
                             stoptime INTEGER NOT NULL
                           ); '''

    insert_record_sql = ''' INSERT INTO records(year, month, user, groupid, cpucount, benchmarktype, benchmarkvalue, recordid, runtime, starttime, stoptime)
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''

    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute(create_table_sql)
    
    for r in records:
        print(r.stop_time)
        print(r.stop_time.timestamp())
        year = r.stop_time.year
        month = r.stop_time.month
        for c in r.components:
            if c.name == 'Cores':
                cpucount = c.amount
                for s in c.scores:
                    if s.name == 'HEPSPEC':
                        benchmark_value = s.factor
                        benchmark_type = s.name

        data_tuple = (year, month, r.user_id, r.group_id, cpucount, benchmark_type, benchmark_value, r.record_id, r.runtime, r.start_time.timestamp(), r.stop_time.timestamp())
        cur.execute(insert_record_sql, data_tuple)

    cur.close()

    return conn

async def create_summary_db(records_db):
    cur = records_db.cursor()
    test_sql = '''SELECT user, year, month, COUNT(recordid), SUM(runtime), MIN(stoptime), MAX(stoptime) FROM records GROUP BY user, year, month, benchmarktype, benchmarkvalue'''
    cur.execute(test_sql)
    pprint(cur.fetchall())

    cur.close()
    records_db.close()

async def merge_records(config, records):
    pass


async def create_summary(config, records):
    await merge_records(config, records)
    pass


async def create_report(config, records):
    site_name = config['site'].get('site_name', fallback=None)

    report = 'APEL-summary-job-message: v0.3\n'
    for r in records:
        print('try apel style')
        if site_name is not None:
            site_id = site_name
        else:
            site_id = r.site_id
        user_id = r.user_id
        report_part = f'''User: {user_id}
Site: {site_id}
%%\n'''
        report += report_part

    print(report)

    return 'nagut'


async def run(config, client):
    run_interval = config['intervals'].getint('run_interval')
    report_interval = config['intervals'].getint('report_interval')

    while True:
        last_report_time = await get_report_time(config)
        current_time = datetime.now()

        # print((current_time-last_report_time).total_seconds())

        if not (current_time-last_report_time).total_seconds() >= report_interval:
            print('Too soon, do nothing for now!')
            await asyncio.sleep(run_interval)
            continue
        else:
            print('Enough time passed since last report, do it again!')

        start_time = await get_start_time(config)

        records = await get_records_since(client, start_time)
        records_db = await create_records_db(records)
        await create_summary_db(records_db)

        try:
            latest_stop_time = records[-1].stop_time
            print('latest_stop_time')
            print(latest_stop_time)
            # report = await create_report(config, records)
            # await send_report(report)
            latest_report_time = datetime.now()
            await update_time_db(config, latest_stop_time, latest_report_time)
        except IndexError:
            print('No new records, do nothing for now!')

        await asyncio.sleep(run_interval)


def main():
    logging.basicConfig(level=logging.DEBUG)

    config = configparser.ConfigParser()
    config.read('apel_plugin.cfg')

    auditor_ip = config['auditor']['auditor_ip']
    auditor_port = config['auditor'].getint('auditor_port')
    builder = AuditorClientBuilder()
    builder = builder.address(auditor_ip, auditor_port)
    client = builder.build()

    try:
        asyncio.run(run(config, client))
    except KeyboardInterrupt:
        print('User abort')
    finally:
        print('APEL plugin stopped')


if __name__ == '__main__':
    main()
