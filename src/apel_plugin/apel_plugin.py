#!/usr/bin/env python3

# SPDX-FileCopyrightText: © 2022 Dirk Sammel <dirk.sammel@gmail.com>
# SPDX-License-Identifier: BSD-2-Clause-Patent

from __future__ import annotations
import logging
import asyncio
from pyauditor import AuditorClientBuilder
from pathlib import Path
import sqlite3
import aiosqlite
from aiosqlite import Error
from datetime import datetime
import configparser
import pytz
import json
import sys
import re
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs7
import base64


async def regex_dict_lookup(term, dict):
    for key in dict:
        if re.search(key, term):
            return dict[key]
    logging.critical(f"Search term {term} not matched in {dict.keys()}")
    sys.exit(1)


async def get_time_db(config):
    time_db_path = config["paths"].get("time_db_path")

    try:
        if Path(time_db_path).is_file():
            conn = await aiosqlite.connect(
                time_db_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
        else:
            conn = await create_time_db(time_db_path)
    except Error as e:
        logging.critical(e)

    return conn


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
                            ?, ?
                        )
                        """
    try:
        conn = await aiosqlite.connect(":memory:")
        cur = await conn.cursor()
        await cur.execute(create_table_sql)
    except Error as e:
        logging.critical(e)

    try:
        site_name_mapping = json.loads(config["site"].get("site_name_mapping"))
    except TypeError:
        site_name_mapping = None

    vo_mapping = json.loads(config["uservo"].get("vo_mapping"))
    submit_host = config["site"].get("submit_host")
    infrastructure = config["site"].get("infrastructure_type")
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
            r.record_id,
            r.runtime,
            norm_runtime,
            r.start_time.replace(tzinfo=pytz.utc).timestamp(),
            r.stop_time.replace(tzinfo=pytz.utc).timestamp(),
        )
        try:
            await cur.execute(insert_record_sql, data_tuple)
        except Error as e:
            logging.critical(e)

    try:
        await conn.commit()
        await cur.close()
    except Error as e:
        logging.critical(e)

    return conn


async def create_summary_db(records_db):
    records_db.row_factory = aiosqlite.Row
    cur = await records_db.cursor()
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
                       SUM(runtime) as runtime,
                       SUM(normruntime) as norm_runtime,
                       MIN(stoptime) as min_stoptime,
                       MAX(stoptime) as max_stoptime
                FROM records
                GROUP BY site,
                         vo,
                         year,
                         month,
                         cpucount
                """
    await cur.execute(group_sql)

    grouped_dict = await cur.fetchall()

    await cur.close()
    await records_db.close()

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
        summary += "NodeCount: 1\n"
        summary += f"EarliestEndTime: {entry['min_stoptime']}\n"
        summary += f"LatestEndTime: {entry['max_stoptime']}\n"
        summary += f"WallDuration : {entry['runtime']}\n"
        summary += f"CpuDuration: {entry['runtime']}\n"
        summary += f"NormalisedWallDuration: {entry['norm_runtime']}\n"
        summary += f"NormalisedCpuDuration: {entry['norm_runtime']}\n"
        summary += f"NumberOfJobs: {entry['jobcount']}\n"
        summary += "%%\n"

    return summary


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


async def sign_msg(config, msg):
    client_cert = config["authentication"].get("client_cert")
    client_key = config["authentication"].get("client_key")

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


async def run(config, client):
    run_interval = config["intervals"].getint("run_interval")
    report_interval = config["intervals"].getint("report_interval")

    token = await get_token(config)
    logging.debug(token)

    while True:
        time_db_conn = await get_time_db(config)
        last_report_time = await get_report_time(time_db_conn)
        current_time = datetime.now()
        time_since_report = (current_time - last_report_time).total_seconds()

        if not time_since_report >= report_interval:
            logging.info("Not enough time since last report")
            await time_db_conn.close()
            await asyncio.sleep(run_interval)
            continue
        else:
            logging.info("Enough time since last report, create new report")

        try:
            start_time = await get_start_time(time_db_conn)
            logging.info(f"Getting records since {start_time}")
            records = await client.get_stopped_since(start_time)

            latest_stop_time = records[-1].stop_time.replace(tzinfo=pytz.utc)
            logging.debug(f"Latest stop time is {latest_stop_time}")

            # maybe move this into one function create_summary(records)?
            records_db = await create_records_db(config, records)
            grouped_dict = await create_summary_db(records_db)
            summary = await create_summary(grouped_dict)
            logging.debug(summary)
            signed_summary = await sign_msg(config, summary)
            logging.debug(signed_summary)
            encoded_summary = base64.b64encode(signed_summary).decode("utf-8")
            logging.debug(encoded_summary)
            payload_summary = await build_payload(encoded_summary)
            logging.debug(payload_summary)
            post = await send_payload(config, token, payload_summary)
            logging.debug(post.status_code)

            latest_report_time = datetime.now()
            await update_time_db(
                time_db_conn, latest_stop_time.timestamp(), latest_report_time
            )
        except IndexError:
            logging.info("No new records, do nothing for now")

        await time_db_conn.close()
        await asyncio.sleep(run_interval)


def main():
    FORMAT = "[%(asctime)s] %(levelname)-8s %(message)s"
    logging.basicConfig(
        # filename="apel_plugin.log",
        encoding="utf-8",
        level=logging.DEBUG,
        format=FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

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
