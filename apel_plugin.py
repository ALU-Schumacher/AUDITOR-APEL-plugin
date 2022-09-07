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
import json
import sys

async def get_records(client):
    response = await client.get()
    return response


async def get_records_since(client, start_time):
    response = await client.get_stopped_since(start_time)
    return response


async def create_time_db(time_db_path):
    create_table_sql = ''' CREATE TABLE IF NOT EXISTS times (
                             target TEXT UNIQUE NOT NULL,
                             last_end_time INTEGER NOT NULL,
                             last_report_time timestamp NOT NULL
                           ); '''

    insert_sql = ''' INSERT INTO times(target, last_end_time, last_report_time)
                     VALUES(?, ?, ?); '''
    ini_time = datetime(1970, 1, 1, 0, 0, 0)
    data_tuple = ('arex/jura/apel:EGI', 0, ini_time)

    try:
        conn = sqlite3.connect(
            time_db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
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
        conn = sqlite3.connect(
            time_db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cur = conn.cursor()
        cur.row_factory = lambda cursor, row: row[0]
        cur.execute('SELECT last_end_time FROM times')
        start_time = datetime.fromtimestamp(cur.fetchall()[0], tz=pytz.utc)
        cur.close()
        conn.close()
        return start_time
    except Error as e:
        print(e)


async def get_report_time(config):
    time_db_path = config['paths']['time_db_path']

    if Path(time_db_path).is_file():
        try:
            conn = sqlite3.connect(
                time_db_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
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
        conn = sqlite3.connect(
            time_db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cur = conn.cursor()
        cur.execute(update_sql, (stop_time, report_time))
        conn.commit()
        cur.close()
        conn.close()
    except Error as e:
        print(e)


async def create_records_db(config, records):
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
                             site TEXT NOT NULL,
                             year INTEGER NOT NULL,
                             month INTEGER NOT NULL,
                             user TEXT NOT NULL,
                             groupid TEXT NOT NULL,
                             cpucount INTEGER NOT NULL,
                             benchmarktype TEXT NOT NULL,
                             benchmarkvalue FLOAT NOT NULL,
                             recordid TEXT UNIQUE NOT NULL,
                             runtime INTEGER NOT NULL,
                             starttime INTEGER NOT NULL,
                             stoptime INTEGER NOT NULL
                           ); '''

    insert_record_sql = ''' INSERT INTO records(site, year, month, user, groupid, cpucount, benchmarktype, benchmarkvalue, recordid, runtime, starttime, stoptime)
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''

    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute(create_table_sql)

    try:
        site_list = json.loads(config['site'].get('site_list'))
    except TypeError:
        site_list = None

    for r in records:
        if site_list is not None:
            try:
                site_id = site_list[r.site_id]
            except KeyError:
                print(f'No site name mapping defined for site {r.site_id}')
                sys.exit(1)
        else:
            site_id = r.site_id
        year = r.stop_time.replace(tzinfo=pytz.utc).year
        month = r.stop_time.replace(tzinfo=pytz.utc).month
        for c in r.components:
            if c.name == 'Cores':
                cpucount = c.amount
                for s in c.scores:
                    if s.name == 'HEPSPEC':
                        benchmark_value = s.factor
                        benchmark_type = s.name

        data_tuple = (
            site_id,
            year,
            month,
            r.user_id,
            r.group_id,
            cpucount,
            benchmark_type,
            benchmark_value,
            r.record_id,
            r.runtime,
            r.start_time.replace(tzinfo=pytz.utc).timestamp(),
            r.stop_time.replace(tzinfo=pytz.utc).timestamp()
        )
        cur.execute(insert_record_sql, data_tuple)

    conn.commit()
    cur.close()

    return conn


async def create_summary_db(records_db):
    records_db.row_factory = sqlite3.Row
    cur = records_db.cursor()
    group_sql = '''SELECT site, user, year, month, cpucount, COUNT(recordid) as jobcount, SUM(runtime) as runtime, MIN(stoptime) as min_stoptime, MAX(stoptime) as max_stoptime FROM records GROUP BY site, user, year, month, benchmarktype, benchmarkvalue, cpucount'''
    cur.execute(group_sql)

    grouped_dict = cur.fetchall()

    cur.close()
    records_db.close()

    return grouped_dict


async def create_summary(grouped_dict):
    summary = 'APEL-summary-job-message: v0.3\n'

    for entry in grouped_dict:
        summary += f'Site: {entry["site"]}\n'
        summary += f'Month: {entry["month"]}\n'
        summary += f'Year: {entry["year"]}\n'
        summary += f'GlobalUserName: ???\n'
        summary += f'VO: ???\n'
        summary += f'VOGroup: ???\n'
        summary += f'VORole: ???\n'
        summary += f'SubmitHost: ???\n'
        summary += f'Infrastructure: ???\n'
        summary += f'Processors: {entry["cpucount"]}\n'
        summary += f'NodeCount: ???\n'
        summary += f'EarliestEndTime: {entry["min_stoptime"]}\n'
        summary += f'LatestEndTime: {entry["max_stoptime"]}\n'
        summary += f'WallDuration : {entry["runtime"]}\n'
        summary += f'CpuDuration: ???\n'
        summary += f'NormalisedWallDuration: ???\n'
        summary += f'NormalisedCpuDuration: ???\n'
        summary += f'NumberOfJobs: {entry["jobcount"]}\n'
        summary += '%%\n'

    return(summary)


async def run(config, client):
    run_interval = config['intervals'].getint('run_interval')
    report_interval = config['intervals'].getint('report_interval')

    while True:
        last_report_time = await get_report_time(config)
        current_time = datetime.now()

        if not (current_time - last_report_time).total_seconds() >= report_interval:
            print('Too soon, do nothing for now!')
            await asyncio.sleep(run_interval)
            continue
        else:
            print('Enough time passed since last report, do it again!')

        try:
            start_time = await get_start_time(config)
            print(f'Getting records since {start_time}')
            records = await get_records_since(client, start_time)
            latest_stop_time = records[-1].stop_time.replace(tzinfo=pytz.utc).timestamp()
            print(f'Latest stop time is {datetime.fromtimestamp(latest_stop_time, tz=pytz.utc)}')

            # maybe move this into one function create_summary(records)?
            records_db = await create_records_db(config, records)
            grouped_dict = await create_summary_db(records_db)
            summary = await create_summary(grouped_dict)
            print(summary)

            # report = await create_report(config, records)
            # await send_summary(summary)
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
