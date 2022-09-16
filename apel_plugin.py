#!/usr/bin/env python3

from __future__ import annotations
import logging
import asyncio
from pyauditor import AuditorClientBuilder
from pathlib import Path
import sqlite3
from sqlite3 import Error
from datetime import datetime
import configparser
import pytz
import json
import sys
import re


async def regex_dict_lookup(term, dict):
    for key in dict:
        if re.search(key, term):
            return dict[key]
    logging.critical(f"Search term {term} not be matched in {dict.keys()}")
    sys.exit(1)


async def get_records(client):
    response = await client.get()
    return response


async def get_records_since(client, start_time):
    response = await client.get_stopped_since(start_time)
    return response


async def create_time_db(time_db_path):
    create_table_sql = """
                       CREATE TABLE IF NOT EXISTS times(
                           target TEXT UNIQUE NOT NULL,
                           last_end_time INTEGER NOT NULL,
                           last_report_time timestamp NOT NULL
                       )
                       """

    insert_sql = """
                 INSERT INTO times(
                     target,
                     last_end_time,
                     last_report_time
                 )
                 VALUES(
                     ?, ?, ?
                 )
                 """

    ini_time = datetime(1970, 1, 1, 0, 0, 0)
    data_tuple = ("arex/jura/apel:EGI", 0, ini_time)

    try:
        conn = sqlite3.connect(
            time_db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        cur = conn.cursor()
        cur.execute(create_table_sql)
        cur.execute(insert_sql, data_tuple)
        conn.commit()
        cur.close()
        conn.close()
        return ini_time
    except Error as e:
        logging.critical(e)


async def get_start_time(config):
    time_db_path = config["paths"].get("time_db_path")

    try:
        conn = sqlite3.connect(
            time_db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        cur = conn.cursor()
        cur.row_factory = lambda cursor, row: row[0]
        cur.execute("SELECT last_end_time FROM times")
        start_time = datetime.fromtimestamp(cur.fetchall()[0], tz=pytz.utc)
        cur.close()
        conn.close()
        return start_time
    except Error as e:
        logging.critical(e)


async def get_report_time(config):
    time_db_path = config["paths"].get("time_db_path")

    if Path(time_db_path).is_file():
        try:
            conn = sqlite3.connect(
                time_db_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
            cur = conn.cursor()
            cur.row_factory = lambda cursor, row: row[0]
            cur.execute("SELECT last_report_time FROM times")
            report_time = cur.fetchall()[0]
            cur.close()
            conn.close()
            return report_time
        except Error as e:
            logging.critical(e)
    else:
        report_time = await create_time_db(time_db_path)
        return report_time


async def update_time_db(config, stop_time, report_time):
    time_db_path = config["paths"].get("time_db_path")

    update_sql = """
                 UPDATE times
                 SET last_end_time = ?,
                     last_report_time = ?
                 """
    try:
        conn = sqlite3.connect(
            time_db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        cur = conn.cursor()
        cur.execute(update_sql, (stop_time, report_time))
        conn.commit()
        cur.close()
        conn.close()
    except Error as e:
        logging.critical(e)


async def create_records_db(config, records):
    create_table_sql = """
                       CREATE TABLE IF NOT EXISTS records(
                           site TEXT NOT NULL,
                           submithost TEXT NOT NULL,
                           vo TEXT NOT NULL,
                           vogroup TEXT NOT NULL,
                           vorole TEXT NOT NULL,
                           infrastructure TEXT NOT NULL,
                           year INTEGER NOT NULL,
                           month INTEGER NOT NULL,
                           cpucount INTEGER NOT NULL,
                           benchmarktype TEXT NOT NULL,
                           benchmarkvalue FLOAT NOT NULL,
                           recordid TEXT UNIQUE NOT NULL,
                           runtime INTEGER NOT NULL,
                           normruntime INTEGER NOT NULL,
                           starttime INTEGER NOT NULL,
                           stoptime INTEGER NOT NULL
                       )
                       """

    insert_record_sql = """
                        INSERT INTO records(
                            site,
                            submithost,
                            vo,
                            vogroup,
                            vorole,
                            infrastructure,
                            year,
                            month,
                            cpucount,
                            benchmarktype,
                            benchmarkvalue,
                            recordid,
                            runtime,
                            normruntime,
                            starttime,
                            stoptime
                        )
                        VALUES(
                            ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?, ?
                        )
                        """
    try:
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute(create_table_sql)
    except Error as e:
        logging.critical(e)

    try:
        site_name_mapping = json.loads(config["site"].get("site_name_mapping"))
    except TypeError:
        site_name_mapping = None

    vo_mapping = json.loads(config["uservo"].get("vo_mapping"))
    submit_host = config["site"].get("submit_host")
    infrastructure = config["site"].get("infrastructure_type")
    benchmark_type = config["site"].get("benchmark_type")
    benchmark_name = config["auditor"].get("benchmark_name")
    cores_name = config["auditor"].get("cores_name")

    for r in records:
        if site_name_mapping is not None:
            try:
                site_name = site_name_mapping[r.site_id]
            except KeyError:
                logging.critical(
                    f"No site name mapping defined for site {r.site_id}"
                )
                sys.exit(1)
        else:
            site_name = r.site_id
        vo_info = await regex_dict_lookup(r.user_id, vo_mapping)
        year = r.stop_time.replace(tzinfo=pytz.utc).year
        month = r.stop_time.replace(tzinfo=pytz.utc).month
        for c in r.components:
            if c.name == cores_name:
                cpucount = c.amount
                for s in c.scores:
                    if s.name == benchmark_name:
                        benchmark_value = s.factor

        norm_runtime = r.runtime * benchmark_value

        data_tuple = (
            site_name,
            submit_host,
            vo_info["vo"],
            vo_info["vogroup"],
            vo_info["vorole"],
            infrastructure,
            year,
            month,
            cpucount,
            benchmark_type,
            benchmark_value,
            r.record_id,
            r.runtime,
            norm_runtime,
            r.start_time.replace(tzinfo=pytz.utc).timestamp(),
            r.stop_time.replace(tzinfo=pytz.utc).timestamp(),
        )
        try:
            cur.execute(insert_record_sql, data_tuple)
        except Error as e:
            logging.critical(e)

    try:
        conn.commit()
        cur.close()
    except Error as e:
        logging.critical(e)

    return conn


async def create_summary_db(records_db):
    records_db.row_factory = sqlite3.Row
    cur = records_db.cursor()
    group_sql = """
                SELECT site,
                       submithost,
                       vo,
                       vogroup,
                       vorole,
                       infrastructure,
                       year,
                       month,
                       cpucount,
                       COUNT(recordid) as jobcount,
                       benchmarktype,
                       benchmarkvalue,
                       SUM(runtime) as runtime,
                       SUM(normruntime) as norm_runtime,
                       MIN(stoptime) as min_stoptime,
                       MAX(stoptime) as max_stoptime
                FROM records
                GROUP BY site,
                         vo,
                         year,
                         month,
                         benchmarktype,
                         benchmarkvalue,
                         cpucount
                """
    cur.execute(group_sql)

    grouped_dict = cur.fetchall()

    cur.close()
    records_db.close()

    return grouped_dict


async def create_summary(grouped_dict):
    summary = "APEL-summary-job-message: v0.3\n"

    for entry in grouped_dict:
        summary += f"Site: {entry['site']}\n"
        summary += f"Month: {entry['month']}\n"
        summary += f"Year: {entry['year']}\n"
        summary += f"VO: {entry['vo']}\n"
        summary += f"VOGroup: {entry['vogroup']}\n"
        summary += f"VORole: {entry['vorole']}\n"
        summary += f"SubmitHost: {entry['submithost']}\n"
        summary += f"Infrastructure: {entry['infrastructure']}\n"
        summary += f"Processors: {entry['cpucount']}\n"
        summary += "NodeCount: ???\n"
        summary += f"ServiceLevelType: {entry['benchmarktype']}\n"
        summary += f"ServiceLevel: {entry['benchmarkvalue']}\n"
        summary += f"EarliestEndTime: {entry['min_stoptime']}\n"
        summary += f"LatestEndTime: {entry['max_stoptime']}\n"
        summary += f"WallDuration : {entry['runtime']}\n"
        summary += "CpuDuration: ???\n"
        summary += f"NormalisedWallDuration: {entry['norm_runtime']}\n"
        summary += "NormalisedCpuDuration: ???\n"
        summary += f"NumberOfJobs: {entry['jobcount']}\n"
        summary += "%%\n"

    return summary


async def run(config, client):
    run_interval = config["intervals"].getint("run_interval")
    report_interval = config["intervals"].getint("report_interval")

    while True:
        last_report_time = await get_report_time(config)
        current_time = datetime.now()
        time_since_report = (current_time - last_report_time).total_seconds()

        if not time_since_report >= report_interval:
            logging.info("Not enough time since last report")
            await asyncio.sleep(run_interval)
            continue
        else:
            logging.info("Enough time since last report, create new report")

        try:
            start_time = await get_start_time(config)
            logging.info(f"Getting records since {start_time}")
            records = await get_records_since(client, start_time)

            latest_stop_time = records[-1].stop_time.replace(tzinfo=pytz.utc)
            logging.debug(f"Latest stop time is {latest_stop_time}")

            # maybe move this into one function create_summary(records)?
            records_db = await create_records_db(config, records)
            grouped_dict = await create_summary_db(records_db)
            summary = await create_summary(grouped_dict)
            logging.debug(summary)
            # await send_summary(summary)

            latest_report_time = datetime.now()
            await update_time_db(
                config, latest_stop_time.timestamp(), latest_report_time
            )
        except IndexError:
            logging.info("No new records, do nothing for now")

        await asyncio.sleep(run_interval)


def main():
    FORMAT = (
        "%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
    )
    logging.basicConfig(
        level=logging.DEBUG, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S"
    )

    config = configparser.ConfigParser()
    config.read("apel_plugin.cfg")

    auditor_ip = config["auditor"].get("auditor_ip")
    auditor_port = config["auditor"].getint("auditor_port")

    builder = AuditorClientBuilder()
    builder = builder.address(auditor_ip, auditor_port)
    client = builder.build()

    try:
        asyncio.run(run(config, client))
    except KeyboardInterrupt:
        logging.critical("User abort")
    finally:
        logging.critical("APEL plugin stopped")


if __name__ == "__main__":
    main()
