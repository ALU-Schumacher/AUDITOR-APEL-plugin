#!/usr/bin/env python3

# SPDX-FileCopyrightText: © 2022 Dirk Sammel <dirk.sammel@gmail.com>
# SPDX-License-Identifier: BSD-2-Clause-Patent

import logging
import asyncio
from pyauditor import AuditorClientBuilder
import configparser
import argparse
from datetime import datetime
import pytz
import base64
from apel_plugin import (
    get_token,
    create_summary_db,
    group_summary_db,
    create_summary,
    sign_msg,
    build_payload,
    send_payload,
)


async def run(config, args, client):
    client_cert = config["authentication"].get("client_cert")
    client_key = config["authentication"].get("client_key")

    month = args.month
    year = args.year
    site = args.site

    begin_month = datetime(year, month, 1).replace(tzinfo=pytz.utc)

    records = await client.get_stopped_since(begin_month)
    token = get_token(config)
    logging.debug(token)

    summary_db = create_summary_db(config, records)
    grouped_summary_list = group_summary_db(
        summary_db, filter_by=(month, year, site)
    )
    summary = create_summary(grouped_summary_list)
    logging.debug(summary)
    signed_summary = sign_msg(client_cert, client_key, summary)
    logging.debug(signed_summary)
    encoded_summary = base64.b64encode(signed_summary).decode("utf-8")
    logging.debug(encoded_summary)
    payload_summary = build_payload(encoded_summary)
    logging.debug(payload_summary)
    post_summary = send_payload(config, token, payload_summary)
    logging.debug(post_summary.status_code)


def main():
    config = configparser.ConfigParser()
    config.read("/etc/auditor-apel-plugin/apel_plugin.cfg")

    log_level = config["logging"].get("log_level")
    log_format = "[%(asctime)s] %(levelname)-8s %(message)s"
    logging.basicConfig(
        encoding="utf-8",
        level=log_level,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("asyncio").setLevel("WARNING")
    logging.getLogger("aiosqlite").setLevel("WARNING")
    logging.getLogger("urllib3").setLevel("WARNING")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y", "--year", type=int, required=True, help="Year: 2020, 2021, ..."
    )
    parser.add_argument(
        "-m", "--month", type=int, required=True, help="Month: 4, 8, 12, ..."
    )
    parser.add_argument(
        "-s", "--site", required=True, help="Site (GOCDB): UNI-FREIBURG, ..."
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
