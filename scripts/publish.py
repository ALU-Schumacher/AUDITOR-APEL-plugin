#!/usr/bin/env python3

# SPDX-FileCopyrightText: © 2022 Dirk Sammel <dirk.sammel@gmail.com>
# SPDX-License-Identifier: BSD-2-Clause-Patent

import logging
import asyncio
from pyauditor import AuditorClientBuilder
from datetime import datetime
import pytz
import configparser
import base64
from apel_plugin import (
    get_token,
    get_time_db,
    get_report_time,
    get_start_time,
    create_summary_db,
    group_summary_db,
    create_summary,
    sign_msg,
    build_payload,
    send_payload,
    update_time_db,
    get_begin_previous_month,
    create_sync_db,
    group_sync_db,
    create_sync,
)


async def run(config, client):
    report_interval = config["intervals"].getint("report_interval")
    time_db_path = config["paths"].get("time_db_path")
    publish_since = config["site"].get("publish_since")

    token = await get_token(config)
    logging.debug(token)

    while True:
        time_db_conn = await get_time_db(publish_since, time_db_path)
        last_report_time = await get_report_time(time_db_conn)
        current_time = datetime.now()
        time_since_report = (current_time - last_report_time).total_seconds()

        if time_since_report < report_interval:
            logging.info("Not enough time since last report")
            await time_db_conn.close()
            await asyncio.sleep(report_interval - time_since_report)
            continue
        else:
            logging.info("Enough time since last report, create new report")

        try:
            start_time = await get_start_time(time_db_conn)
            logging.info(f"Getting records since {start_time}")
            records_summary = await client.get_stopped_since(start_time)
            latest_stop_time = records_summary[-1].stop_time.replace(
                tzinfo=pytz.utc
            )
            logging.debug(f"Latest stop time is {latest_stop_time}")
            summary_db = await create_summary_db(config, records_summary)
            grouped_summary_list = await group_summary_db(summary_db)
            summary = await create_summary(grouped_summary_list)
            logging.debug(summary)
            signed_summary = await sign_msg(config, summary)
            logging.debug(signed_summary)
            encoded_summary = base64.b64encode(signed_summary).decode("utf-8")
            logging.debug(encoded_summary)
            payload_summary = await build_payload(encoded_summary)
            logging.debug(payload_summary)
            post_summary = await send_payload(config, token, payload_summary)
            logging.debug(post_summary.status_code)

            begin_previous_month = await get_begin_previous_month(current_time)
            records_sync = await client.get_stopped_since(begin_previous_month)
            sync_db = await create_sync_db(config, records_sync)
            grouped_sync_list = await group_sync_db(sync_db)
            sync = await create_sync(grouped_sync_list)
            logging.debug(sync)
            signed_sync = await sign_msg(config, sync)
            logging.debug(signed_sync)
            encoded_sync = base64.b64encode(signed_sync).decode("utf-8")
            logging.debug(encoded_sync)
            payload_sync = await build_payload(encoded_sync)
            logging.debug(payload_sync)
            post_sync = await send_payload(config, token, payload_sync)
            logging.debug(post_sync.status_code)

            latest_report_time = datetime.now()
            await update_time_db(
                time_db_conn, latest_stop_time.timestamp(), latest_report_time
            )
        except IndexError:
            logging.info("No new records, do nothing for now")

        await time_db_conn.close()
        await asyncio.sleep(report_interval)


def main():
    config = configparser.ConfigParser()
    config.read("apel_plugin.cfg")

    log_level = config["logging"].get("log_level")
    log_format = (
        "[%(asctime)s] %(levelname)-8s %(message)s "
        "(%(pathname)s at line %(lineno)d)"
    )
    logging.basicConfig(
        # filename="apel_plugin.log",
        encoding="utf-8",
        level=log_level,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("asyncio").setLevel("WARNING")
    logging.getLogger("aiosqlite").setLevel("WARNING")
    logging.getLogger("urllib3").setLevel("WARNING")

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
