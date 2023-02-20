#!/usr/bin/env python3

# SPDX-FileCopyrightText: © 2022 Dirk Sammel <dirk.sammel@gmail.com>
# SPDX-License-Identifier: BSD-2-Clause-Patent

import logging
from pathlib import Path
import sqlite3
import aiosqlite
from aiosqlite import Error
from datetime import datetime, timedelta, time
import pytz
import json
import sys
import re
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs7


async def get_begin_previous_month(current_time):
    first_current_month = current_time.replace(day=1)
    previous_month = first_current_month - timedelta(days=1)
    first_previous_month = previous_month.replace(day=1)
    begin_previous_month = datetime.combine(first_previous_month, time())
    begin_previous_month_utc = begin_previous_month.replace(tzinfo=pytz.utc)

    return begin_previous_month_utc


async def regex_dict_lookup(term, dict):
    result = None
    for key in dict:
        if re.search(key, term):
            result = dict[key]

    return result


async def get_time_db(publish_since, time_db_path):
    if Path(time_db_path).is_file():
        conn = await aiosqlite.connect(
            time_db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
    else:
        conn = await create_time_db(publish_since, time_db_path)

    return conn


async def create_time_db(publish_since, time_db_path):
    create_table_sql = """
                       CREATE TABLE IF NOT EXISTS times(
                           last_end_time INTEGER NOT NULL,
                           last_report_time timestamp NOT NULL
                       )
                       """

    insert_sql = """
                 INSERT INTO times(
                     last_end_time,
                     last_report_time
                 )
                 VALUES(
                     ?, ?
                 )
                 """

    initial_report_time = datetime(1970, 1, 1, 0, 0, 0)
    publish_since_datetime = datetime.strptime(
        publish_since, "%Y-%m-%d %H:%M:%S%z"
    )
    data_tuple = (
        publish_since_datetime.replace(tzinfo=pytz.utc).timestamp(),
        initial_report_time,
    )

    try:
        conn = await aiosqlite.connect(
            time_db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        cur = await conn.cursor()
        await cur.execute(create_table_sql)
        await cur.execute(insert_sql, data_tuple)
        await conn.commit()
        await cur.close()
        return conn
    except Error as e:
        logging.critical(e)
        raise e


async def get_start_time(conn):
    try:
        cur = await conn.cursor()
        cur.row_factory = lambda cursor, row: row[0]
        await cur.execute("SELECT last_end_time FROM times")
        start_time_row = await cur.fetchall()
        start_time = datetime.fromtimestamp(start_time_row[0][0], tz=pytz.utc)
        await cur.close()
        return start_time
    except Error as e:
        logging.critical(e)
        raise e


async def get_report_time(conn):
    try:
        cur = await conn.cursor()
        cur.row_factory = lambda cursor, row: row[0]
        await cur.execute("SELECT last_report_time FROM times")
        report_time_row = await cur.fetchall()
        report_time = report_time_row[0][0]
        await cur.close()
        return report_time
    except Error as e:
        logging.critical(e)
        raise e


async def update_time_db(conn, stop_time, report_time):
    update_sql = """
                 UPDATE times
                 SET last_end_time = ?,
                     last_report_time = ?
                 """
    try:
        cur = await conn.cursor()
        await cur.execute(update_sql, (stop_time, report_time))
        await conn.commit()
        await cur.close()
    except Error as e:
        logging.critical(e)
        raise e


async def create_summary_db(config, records):
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
                           nodecount INTEGER NOT NULL,
                           recordid TEXT UNIQUE NOT NULL,
                           runtime INTEGER NOT NULL,
                           normruntime INTEGER NOT NULL,
                           cputime INTEGER NOT NULL,
                           normcputime INTEGER NOT NULL,
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
                            nodecount,
                            recordid,
                            runtime,
                            normruntime,
                            cputime,
                            normcputime,
                            starttime,
                            stoptime
                        )
                        VALUES(
                            ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?
                        )
                        """

    try:
        conn = await aiosqlite.connect(":memory:")
        cur = await conn.cursor()
        await cur.execute(create_table_sql)
    except Error as e:
        logging.critical(e)
        raise e

    try:
        site_name_mapping = json.loads(config["site"].get("site_name_mapping"))
    except TypeError:
        site_name_mapping = None

    sites_to_report = json.loads(config["site"].get("sites_to_report"))
    vo_mapping = json.loads(config["uservo"].get("vo_mapping"))
    submit_host = config["site"].get("submit_host")
    infrastructure = config["site"].get("infrastructure_type")
    benchmark_name = config["auditor"].get("benchmark_name")
    cores_name = config["auditor"].get("cores_name")
    cpu_time_name = config["auditor"].get("cpu_time_name")
    nnodes_name = config["auditor"].get("nnodes_name")
    meta_key_site = config["auditor"].get("meta_key_site")
    meta_key_user = config["auditor"].get("meta_key_user")

    for r in records:
        site_id = r.meta.get(meta_key_site)[0]
        if site_id not in sites_to_report:
            continue

        if site_name_mapping is not None:
            try:
                site_name = site_name_mapping[site_id]
            except KeyError:
                logging.critical(
                    f"No site name mapping defined for site {site_id}"
                )
                sys.exit(1)
        else:
            site_name = site_id

        user_id = r.meta.get(meta_key_user)[0]
        vo_info = await regex_dict_lookup(user_id, vo_mapping)
        if vo_info is None:
            logging.critical(
                f"User {user_id} not matched in {vo_mapping.keys()}"
            )
            raise KeyError

        year = r.stop_time.replace(tzinfo=pytz.utc).year
        month = r.stop_time.replace(tzinfo=pytz.utc).month
        for c in r.components:
            if c.name == cores_name:
                cpucount = c.amount
                for s in c.scores:
                    if s.name == benchmark_name:
                        benchmark_value = s.value
            elif c.name == cpu_time_name:
                cputime = c.amount
            elif c.name == nnodes_name:
                nodecount = c.amount

        norm_runtime = r.runtime * benchmark_value
        norm_cputime = cputime * benchmark_value

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
            nodecount,
            r.record_id,
            r.runtime,
            norm_runtime,
            cputime,
            norm_cputime,
            r.start_time.replace(tzinfo=pytz.utc).timestamp(),
            r.stop_time.replace(tzinfo=pytz.utc).timestamp(),
        )
        try:
            await cur.execute(insert_record_sql, data_tuple)
        except Error as e:
            logging.critical(e)
            raise e

    try:
        await conn.commit()
        await cur.close()
    except Error as e:
        logging.critical(e)
        raise e

    return conn


async def create_sync_db(config, records):
    create_table_sql = """
                       CREATE TABLE IF NOT EXISTS records(
                           site TEXT NOT NULL,
                           submithost TEXT NOT NULL,
                           year INTEGER NOT NULL,
                           month INTEGER NOT NULL,
                           recordid TEXT UNIQUE NOT NULL
                       )
                       """

    insert_record_sql = """
                        INSERT INTO records(
                            site,
                            submithost,
                            year,
                            month,
                            recordid
                        )
                        VALUES(
                            ?, ?, ?, ?, ?
                        )
                        """

    try:
        conn = await aiosqlite.connect(":memory:")
        cur = await conn.cursor()
        await cur.execute(create_table_sql)
    except Error as e:
        logging.critical(e)
        raise e

    try:
        site_name_mapping = json.loads(config["site"].get("site_name_mapping"))
    except TypeError:
        site_name_mapping = None

    sites_to_report = json.loads(config["site"].get("sites_to_report"))
    submit_host = config["site"].get("submit_host")
    meta_key_site = config["auditor"].get("meta_key_site")

    for r in records:
        site_id = r.meta.get(meta_key_site)[0]

        if site_id not in sites_to_report:
            continue
        if site_name_mapping is not None:
            try:
                site_name = site_name_mapping[site_id]
            except KeyError:
                logging.critical(
                    f"No site name mapping defined for site {site_id}"
                )
                sys.exit(1)
        else:
            site_name = site_id

        year = r.stop_time.replace(tzinfo=pytz.utc).year
        month = r.stop_time.replace(tzinfo=pytz.utc).month

        data_tuple = (
            site_name,
            submit_host,
            year,
            month,
            r.record_id,
        )
        try:
            await cur.execute(insert_record_sql, data_tuple)
        except Error as e:
            logging.critical(e)
            raise e

    try:
        await conn.commit()
        await cur.close()
    except Error as e:
        logging.critical(e)
        raise e

    return conn


async def group_summary_db(summary_db, filter_by: (int, int, str) = None):
    filter = ""
    if filter_by is not None:
        filter = f"""
                  WHERE month IS {filter_by[0]}
                  AND year IS {filter_by[1]}
                  AND site IS '{filter_by[2]}'
                  """

    group_sql = f"""
                 SELECT site,
                        submithost,
                        vo,
                        vogroup,
                        vorole,
                        infrastructure,
                        year,
                        month,
                        cpucount,
                        nodecount,
                        COUNT(recordid) as jobcount,
                        SUM(runtime) as runtime,
                        SUM(normruntime) as norm_runtime,
                        SUM(cputime) as cputime,
                        SUM(normcputime) as norm_cputime,
                        MIN(stoptime) as min_stoptime,
                        MAX(stoptime) as max_stoptime
                 FROM records
                 {filter}
                 GROUP BY site,
                          submithost,
                          vo,
                          year,
                          month,
                          cpucount,
                          nodecount
                 """

    summary_db.row_factory = aiosqlite.Row
    cur = await summary_db.cursor()
    await cur.execute(group_sql)
    grouped_summary_list = await cur.fetchall()
    await cur.close()
    await summary_db.close()

    return grouped_summary_list


async def group_sync_db(sync_db):
    sync_db.row_factory = aiosqlite.Row
    cur = await sync_db.cursor()
    group_sql = """
                SELECT site,
                       submithost,
                       year,
                       month,
                       COUNT(recordid) as jobcount
                FROM records
                GROUP BY site,
                         submithost,
                         year,
                         month
                """

    await cur.execute(group_sql)
    grouped_sync_list = await cur.fetchall()
    await cur.close()
    await sync_db.close()

    return grouped_sync_list


async def create_summary(grouped_summary_list):
    summary = "APEL-summary-job-message: v0.3\n"

    for entry in grouped_summary_list:
        summary += f"Site: {entry['site']}\n"
        summary += f"Month: {entry['month']}\n"
        summary += f"Year: {entry['year']}\n"
        summary += f"VO: {entry['vo']}\n"
        summary += f"VOGroup: {entry['vogroup']}\n"
        summary += f"VORole: {entry['vorole']}\n"
        summary += f"SubmitHost: {entry['submithost']}\n"
        summary += f"Infrastructure: {entry['infrastructure']}\n"
        summary += f"Processors: {entry['cpucount']}\n"
        summary += f"NodeCount: {entry['nodecount']}\n"
        summary += f"EarliestEndTime: {entry['min_stoptime']}\n"
        summary += f"LatestEndTime: {entry['max_stoptime']}\n"
        summary += f"WallDuration : {entry['runtime']}\n"
        summary += f"CpuDuration: {entry['cputime']}\n"
        summary += f"NormalisedWallDuration: {entry['norm_runtime']}\n"
        summary += f"NormalisedCpuDuration: {entry['norm_cputime']}\n"
        summary += f"NumberOfJobs: {entry['jobcount']}\n"
        summary += "%%\n"

    return summary


async def create_sync(sync_db):
    sync = "APEL-sync-message: v0.1\n"

    for entry in sync_db:
        sync += f"Site: {entry['site']}\n"
        sync += f"Month: {entry['month']}\n"
        sync += f"Year: {entry['year']}\n"
        sync += f"SubmitHost: {entry['submithost']}\n"
        sync += f"NumberOfJobs: {entry['jobcount']}\n"
        sync += "%%\n"

    return sync


async def get_token(config):
    auth_url = config["authentication"].get("auth_url")
    client_cert = config["authentication"].get("client_cert")
    client_key = config["authentication"].get("client_key")
    # ca_path = config["authentication"].get("ca_path")

    response = requests.get(
        auth_url, cert=(client_cert, client_key), verify=False
    )
    token = response.json()["token"]

    return token


async def sign_msg(client_cert, client_key, msg):
    with open(client_cert, "rb") as cc:
        cert = x509.load_pem_x509_certificate(cc.read())

    with open(client_key, "rb") as ck:
        key = serialization.load_pem_private_key(ck.read(), None)

    options = [pkcs7.PKCS7Options.DetachedSignature, pkcs7.PKCS7Options.Text]

    signed_msg = (
        pkcs7.PKCS7SignatureBuilder()
        .set_data(bytes(msg, "utf-8"))
        .add_signer(cert, key, hashes.SHA256())
        .sign(serialization.Encoding.SMIME, options)
    )

    return signed_msg


async def build_payload(msg):
    current_time = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    empaid = f"{current_time[:8]}/{current_time}"

    payload = {"messages": [{"attributes": {"empaid": empaid}, "data": msg}]}

    return payload


async def send_payload(config, token, payload):
    # ca_path = config["authentication"].get("ca_path")
    ams_url = config["authentication"].get("ams_url")
    logging.debug(f"{ams_url}{token}")
    post = requests.post(
        f"{ams_url}{token}",
        json=payload,
        headers={"Content-Type": "application/json"},
        verify=False,
    )

    return post
