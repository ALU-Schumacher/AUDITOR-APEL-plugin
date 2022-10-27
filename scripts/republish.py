#!/usr/bin/env python3

# SPDX-FileCopyrightText: © 2022 Dirk Sammel <dirk.sammel@gmail.com>
# SPDX-License-Identifier: BSD-2-Clause-Patent

from __future__ import annotations
import logging
import asyncio
from pyauditor import AuditorClientBuilder
import configparser
import argparse
from datetime import datetime
import pytz
import base64
import apel_plugin.apel_plugin as apel_plugin


async def run(config, args, client):
    month = args.month
    year = args.year

    begin_month = datetime(year, month, 1).replace(tzinfo=pytz.utc)

    records = await client.get_stopped_since(begin_month)
    token = await apel_plugin.get_token(config)
    logging.debug(token)

    summary_db = await apel_plugin.create_summary_db(config, records)
    grouped_summary_list = await apel_plugin.group_summary_db(summary_db)
    summary = await apel_plugin.create_summary(grouped_summary_list)
    logging.debug(summary)
    signed_summary = await apel_plugin.sign_msg(config, summary)
    logging.debug(signed_summary)
    encoded_summary = base64.b64encode(signed_summary).decode("utf-8")
    logging.debug(encoded_summary)
    payload_summary = await apel_plugin.build_payload(encoded_summary)
    logging.debug(payload_summary)
    post_summary = await apel_plugin.send_payload(
        config, token, payload_summary
    )
    logging.debug(post_summary.status_code)


def main():
    FORMAT = "[%(asctime)s] %(levelname)-8s %(message)s"
    logging.basicConfig(
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

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y", "--year", type=int, required=True, help="year: 2020, 2021, ..."
    )
    parser.add_argument(
        "-m", "--month", type=int, required=True, help="month: 4, 8, 12, ..."
    )
    args = parser.parse_args()

    auditor_ip = config["auditor"].get("auditor_ip")
    auditor_port = config["auditor"].getint("auditor_port")

    builder = AuditorClientBuilder()
    builder = builder.address(auditor_ip, auditor_port)
    client = builder.build()

    try:
        asyncio.run(run(config, args, client))
    except KeyboardInterrupt:
        logging.critical("User abort")
    finally:
        logging.info("Republishing finished")


if __name__ == "__main__":
    main()
