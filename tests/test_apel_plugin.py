import pytest
from apel_plugin import (
    regex_dict_lookup,
    get_begin_previous_month,
    create_time_db,
    get_time_db,
    sign_msg,
    get_start_time,
    get_report_time,
    update_time_db,
    create_summary_db,
)
from datetime import datetime
import pytz
import aiosqlite
import os
import subprocess
import configparser
import pyauditor
from unittest.mock import patch, PropertyMock
import ast


def create_rec(rec_values, conf):
    rec = pyauditor.Record(rec_values["rec_id"], rec_values["start_time"])
    rec.with_stop_time(rec_values["stop_time"])
    rec.with_component(
        pyauditor.Component(
            conf["cores_name"], rec_values["n_cores"]
        ).with_score(
            pyauditor.Score(conf["benchmark_name"], rec_values["hepscore"])
        )
    )
    rec.with_component(
        pyauditor.Component(conf["cpu_time_name"], rec_values["tot_cpu"])
    )
    rec.with_component(
        pyauditor.Component(conf["nnodes_name"], rec_values["n_nodes"])
    )
    meta = pyauditor.Meta()
    meta.insert(conf["meta_key_site"], [rec_values["site"]])
    meta.insert(conf["meta_key_user"], [rec_values["user"]])
    rec.with_meta(meta)

    return rec


@pytest.mark.asyncio
class TestAPELPlugin:
    async def test_regex_dict_lookup(self):
        term_a = "apple"
        term_b = "banana"
        term_c = "citrus"

        dict = {"^a": "apple_in_dict", "^b": "banana_in_dict"}

        result = regex_dict_lookup(term_a, dict)
        assert result == "apple_in_dict"

        result = regex_dict_lookup(term_b, dict)
        assert result == "banana_in_dict"

        result = regex_dict_lookup(term_c, dict)
        assert result is None

    async def test_get_begin_previous_month(self):
        time_a = datetime(2022, 10, 23, 12, 23, 55)
        time_b = datetime(1970, 1, 1, 00, 00, 00)

        result = await get_begin_previous_month(time_a)
        assert result == datetime(2022, 9, 1, 00, 00, 00, tzinfo=pytz.utc)

        result = await get_begin_previous_month(time_b)
        assert result == datetime(1969, 12, 1, 00, 00, 00, tzinfo=pytz.utc)

    async def test_create_time_db(self):
        path = ":memory:"
        publish_since_list = [
            "1970-01-01 00:00:00+00:00",
            "2020-01-01 17:23:00+00:00",
            "2022-12-17 20:20:20+01:00",
        ]

        for publish_since in publish_since_list:
            time_db = await create_time_db(publish_since, path)
            cur = await time_db.cursor()
            await cur.execute("SELECT * FROM times")
            result = await cur.fetchall()
            await cur.close()
            await time_db.close()
            time_dt = datetime.strptime(publish_since, "%Y-%m-%d %H:%M:%S%z")
            time_stamp = time_dt.replace(tzinfo=pytz.utc).timestamp()

            assert result == [(time_stamp, datetime(1970, 1, 1, 0, 0, 0))]

    async def test_create_time_db_fail(self):
        path = "/home/nonexistent/55/abc/time.db"
        publish_since = "1970-01-01 00:00:00+00:00"

        with pytest.raises(Exception) as pytest_error:
            await create_time_db(publish_since, path)
        assert pytest_error.type == aiosqlite.OperationalError

        publish_since = "1970-01-01"

        with pytest.raises(Exception) as pytest_error:
            await create_time_db(publish_since, path)
        assert pytest_error.type == ValueError

    async def test_get_time_db(self):
        path = "/tmp/nonexistent_55_abc_time.db"
        publish_since_list = [
            "1970-01-01 00:00:00+00:00",
            "2020-01-01 17:23:00+00:00",
            "2022-12-17 20:20:20+01:00",
        ]

        for publish_since in publish_since_list:
            time_db = await get_time_db(publish_since, path)
            cur = await time_db.cursor()
            await cur.execute("SELECT * FROM times")
            result = await cur.fetchall()
            await cur.close()
            await time_db.close()
            time_dt = datetime.strptime(publish_since, "%Y-%m-%d %H:%M:%S%z")
            time_stamp = time_dt.replace(tzinfo=pytz.utc).timestamp()
            os.remove(path)

            assert result == [(time_stamp, datetime(1970, 1, 1, 0, 0, 0))]

        for publish_since in publish_since_list:
            time_db = await create_time_db(publish_since, path)
            await time_db.close()
            time_db = await get_time_db(publish_since, path)
            cur = await time_db.cursor()
            await cur.execute("SELECT * FROM times")
            result = await cur.fetchall()
            await cur.close()
            await time_db.close()
            time_dt = datetime.strptime(publish_since, "%Y-%m-%d %H:%M:%S%z")
            time_stamp = time_dt.replace(tzinfo=pytz.utc).timestamp()
            os.remove(path)

            assert result == [(time_stamp, datetime(1970, 1, 1, 0, 0, 0))]

    async def test_sign_msg(self):
        result = await sign_msg(
            "tests/test_cert.cert", "tests/test_key.key", "test"
        )

        with open("/tmp/signed_msg.txt", "wb") as msg_file:
            msg_file.write(result)

        bashCommand = "openssl smime -verify -in /tmp/signed_msg.txt -noverify"
        process = subprocess.Popen(
            bashCommand.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        process.communicate()

        assert process.returncode == 0

    async def test_sign_msg_fail(self):
        with pytest.raises(Exception) as pytest_error:
            await sign_msg(
                "tests/nofolder/test_cert.cert",
                "tests/no/folder/test_key.key",
                "test",
            )
        assert pytest_error.type == FileNotFoundError

        result = await sign_msg(
            "tests/test_cert.cert", "tests/test_key.key", "test"
        )

        with open("/tmp/signed_msg.txt", "wb") as msg_file:
            msg_file.write(result.replace(b"test", b"TEST"))

        bashCommand = "openssl smime -verify -in /tmp/signed_msg.txt -noverify"
        process = subprocess.Popen(
            bashCommand.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        process.communicate()

        assert process.returncode == 4

    async def test_get_start_time(self):
        path = ":memory:"
        publish_since_list = [
            "1970-01-01 00:00:00+00:00",
            "2020-01-01 17:23:00+00:00",
            "2022-12-17 20:20:20+01:00",
        ]

        for publish_since in publish_since_list:
            time_db = await create_time_db(publish_since, path)
            result = await get_start_time(time_db)
            await time_db.close()
            time_dt = datetime.strptime(publish_since, "%Y-%m-%d %H:%M:%S%z")
            time_dt_utc = time_dt.replace(tzinfo=pytz.utc)

            assert result == time_dt_utc

    async def test_get_start_time_fail(self):
        path = ":memory:"
        publish_since = "1970-01-01 00:00:00+00:00"

        time_db = await create_time_db(publish_since, path)
        drop_column = "ALTER TABLE times DROP last_end_time"

        cur = await time_db.cursor()
        await cur.execute(drop_column)
        await time_db.commit()
        await cur.close()
        with pytest.raises(Exception) as pytest_error:
            await get_start_time(time_db)
        await time_db.close()

        assert pytest_error.type == aiosqlite.OperationalError

    async def test_get_report_time(self):
        path = ":memory:"
        publish_since = "1970-01-01 00:00:00+00:00"

        time_db = await create_time_db(publish_since, path)
        result = await get_report_time(time_db)
        await time_db.close()

        initial_report_time = datetime(1970, 1, 1, 0, 0, 0)

        assert result == initial_report_time

    async def test_get_report_time_fail(self):
        path = ":memory:"
        publish_since = "1970-01-01 00:00:00+00:00"

        time_db = await create_time_db(publish_since, path)
        drop_column = "ALTER TABLE times DROP last_report_time"

        cur = await time_db.cursor()
        await cur.execute(drop_column)
        await time_db.commit()
        await cur.close()
        with pytest.raises(Exception) as pytest_error:
            await get_report_time(time_db)
        await time_db.close()

        assert pytest_error.type == aiosqlite.OperationalError

    async def test_update_time_db(self):
        path = ":memory:"
        publish_since = "1970-01-01 00:00:00+00:00"

        time_db = await create_time_db(publish_since, path)
        cur = await time_db.cursor()
        cur.row_factory = lambda cursor, row: row[0]

        stop_time_list = [
            datetime(1984, 3, 3, 0, 0, 0),
            datetime(2022, 12, 23, 12, 44, 23),
            datetime(1999, 10, 1, 23, 17, 45),
        ]
        report_time_list = [
            datetime(1993, 4, 4, 0, 0, 0),
            datetime(2100, 8, 19, 14, 16, 11),
            datetime(1887, 2, 27, 0, 11, 31),
        ]

        for stop_time in stop_time_list:
            for report_time in report_time_list:
                await update_time_db(time_db, stop_time, report_time)

                await cur.execute("SELECT last_end_time FROM times")
                last_end_time_row = await cur.fetchall()
                last_end_time = last_end_time_row[0][0]

                assert last_end_time == stop_time.strftime("%Y-%m-%d %H:%M:%S")

                await cur.execute("SELECT last_report_time FROM times")
                last_report_time_row = await cur.fetchall()
                last_report_time = last_report_time_row[0][0]

                assert last_report_time == report_time

                await update_time_db(
                    time_db, stop_time.timestamp(), report_time
                )

                await cur.execute("SELECT last_end_time FROM times")
                last_end_time_row = await cur.fetchall()
                last_end_time = last_end_time_row[0][0]

                assert last_end_time == stop_time.timestamp()

        await cur.close()
        await time_db.close()

    async def test_update_time_db_fail(self):
        path = ":memory:"
        publish_since = "1970-01-01 00:00:00+00:00"

        time_db = await create_time_db(publish_since, path)
        cur = await time_db.cursor()
        cur.row_factory = lambda cursor, row: row[0]

        stop_time = datetime(1984, 3, 3, 0, 0, 0)
        report_time = datetime(2032, 11, 5, 12, 12, 15).timestamp()

        await update_time_db(time_db, stop_time, report_time)

        with pytest.raises(Exception) as pytest_error:
            await cur.execute("SELECT last_report_time FROM times")

        assert pytest_error.type == ValueError

        drop_column = "ALTER TABLE times DROP last_report_time"
        await cur.execute(drop_column)
        await time_db.commit()

        with pytest.raises(Exception) as pytest_error:
            await update_time_db(time_db, stop_time, report_time)

        assert pytest_error.type == aiosqlite.OperationalError

        await cur.close()
        await time_db.close()

    async def test_create_summary_db(self):
        site_name_mapping = (
            '{"test-site-1": "TEST_SITE_1", "test-site-2": "TEST_SITE_2"}'
        )
        sites_to_report = '["test-site-1", "test-site-2"]'
        submit_host = "https://xxx.test.submit_host.de:1234/xxx"
        infrastructure_type = "grid"
        benchmark_name = "HEPSPEC06"
        cores_name = "Cores"
        cpu_time_name = "TotalCPU"
        nnodes_name = "NNodes"
        meta_key_site = "site_id"
        meta_key_user = "user_id"
        vo_mapping = (
            '{"^first": {"vo": "first_vo", "vogroup": "/first_vogroup", '
            '"vorole": "Role=NULL"}, "^second": {"vo": "second_vo", '
            '"vogroup": "/second_vogroup", "vorole": "Role=production"}}'
        )

        conf = configparser.ConfigParser()
        conf["site"] = {
            "site_name_mapping": site_name_mapping,
            "sites_to_report": sites_to_report,
            "submit_host": submit_host,
            "infrastructure_type": infrastructure_type,
        }
        conf["auditor"] = {
            "benchmark_name": benchmark_name,
            "cores_name": cores_name,
            "cpu_time_name": cpu_time_name,
            "nnodes_name": nnodes_name,
            "meta_key_site": meta_key_site,
            "meta_key_user": meta_key_user,
        }
        conf["uservo"] = {"vo_mapping": vo_mapping}

        runtime = 55

        rec_1_values = {
            "rec_id": "test_record_1",
            "start_time": datetime(1984, 3, 3, 0, 0, 0),
            "stop_time": datetime(1985, 3, 3, 0, 0, 0),
            "n_cores": 8,
            "hepscore": 10.0,
            "tot_cpu": 15520000,
            "n_nodes": 1,
            "site": "test-site-1",
            "user": "first_user",
        }

        rec_2_values = {
            "rec_id": "test_record_2",
            "start_time": datetime(2023, 1, 1, 14, 24, 11),
            "stop_time": datetime(2023, 1, 2, 7, 11, 45),
            "n_cores": 1,
            "hepscore": 23.0,
            "tot_cpu": 12234325,
            "n_nodes": 2,
            "site": "test-site-2",
            "user": "second_user",
        }

        rec_value_list = [rec_1_values, rec_2_values]
        records = []

        with patch(
            "pyauditor.Record.runtime", new_callable=PropertyMock
        ) as mocked_runtime:
            mocked_runtime.return_value = runtime

            for r_values in rec_value_list:
                rec = create_rec(r_values, conf["auditor"])
                records.append(rec)

            result = create_summary_db(conf, records)

        cur = result.cursor()

        cur.execute("SELECT * FROM records")
        content = cur.fetchall()

        cur.close()
        result.close()

        for idx, rec_values in enumerate(rec_value_list):
            assert (
                content[idx][0]
                == ast.literal_eval(site_name_mapping)[rec_values["site"]]
            )
            assert content[idx][1] == submit_host
            assert (
                content[idx][2]
                == regex_dict_lookup(
                    rec_values["user"], ast.literal_eval(vo_mapping)
                )["vo"]
            )
            assert (
                content[idx][3]
                == regex_dict_lookup(
                    rec_values["user"], ast.literal_eval(vo_mapping)
                )["vogroup"]
            )
            assert (
                content[idx][4]
                == regex_dict_lookup(
                    rec_values["user"], ast.literal_eval(vo_mapping)
                )["vorole"]
            )
            assert content[idx][5] == infrastructure_type
            assert content[idx][6] == rec_values["stop_time"].year
            assert content[idx][7] == rec_values["stop_time"].month
            assert content[idx][8] == rec_values["n_cores"]
            assert content[idx][9] == rec_values["n_nodes"]
            assert content[idx][10] == rec_values["rec_id"]
            assert content[idx][11] == runtime
            assert content[idx][12] == runtime * rec_values["hepscore"]
            assert content[idx][13] == rec_values["tot_cpu"]
            assert (
                content[idx][14]
                == rec_values["tot_cpu"] * rec_values["hepscore"]
            )
            assert (
                content[idx][15]
                == rec_values["start_time"]
                .replace(tzinfo=pytz.utc)
                .timestamp()
            )
            assert (
                content[idx][16]
                == rec_values["stop_time"].replace(tzinfo=pytz.utc).timestamp()
            )

        conf["site"] = {
            "sites_to_report": sites_to_report,
            "submit_host": submit_host,
            "infrastructure_type": infrastructure_type,
        }

        records = []

        with patch(
            "pyauditor.Record.runtime", new_callable=PropertyMock
        ) as mocked_runtime:
            mocked_runtime.return_value = runtime

            for r_values in rec_value_list:
                rec = create_rec(r_values, conf["auditor"])
                records.append(rec)

            result = create_summary_db(conf, records)

        cur = result.cursor()

        cur.execute("SELECT * FROM records")
        content = cur.fetchall()

        for idx, rec_values in enumerate(rec_value_list):
            assert content[idx][0] == rec_values["site"]

        cur.close()
        result.close()

        rec_2_values["site"] = "test-site-3"
        records = []

        with patch(
            "pyauditor.Record.runtime", new_callable=PropertyMock
        ) as mocked_runtime:
            mocked_runtime.return_value = runtime

            for r_values in rec_value_list:
                rec = create_rec(r_values, conf["auditor"])
                records.append(rec)

            result = create_summary_db(conf, records)

        cur = result.cursor()

        cur.execute("SELECT * FROM records")
        content = cur.fetchall()

        assert len(content) == 1

        cur.close()
        result.close()

    async def test_create_summary_db_fail(self):
        site_name_mapping = (
            '{"test-site-1": "TEST_SITE_1", "test-site-2": "TEST_SITE_2"}'
        )
        sites_to_report = '["test-site-1", "test-site-2"]'
        submit_host = "https://xxx.test.submit_host.de:1234/xxx"
        infrastructure_type = "grid"
        benchmark_name = "HEPSPEC06"
        cores_name = "Cores"
        cpu_time_name = "TotalCPU"
        nnodes_name = "NNodes"
        meta_key_site = "site_id"
        meta_key_user = "user_id"
        vo_mapping = (
            '{"^first": {"vo": "first_vo", "vogroup": "/first_vogroup", '
            '"vorole": "Role=NULL"}, "^second": {"vo": "second_vo", '
            '"vogroup": "/second_vogroup", "vorole": "Role=production"}}'
        )

        conf = configparser.ConfigParser()
        conf["site"] = {
            "site_name_mapping": site_name_mapping,
            "sites_to_report": sites_to_report,
            "submit_host": submit_host,
            "infrastructure_type": infrastructure_type,
        }
        conf["auditor"] = {
            "benchmark_name": benchmark_name,
            "cores_name": cores_name,
            "cpu_time_name": cpu_time_name,
            "nnodes_name": nnodes_name,
            "meta_key_site": meta_key_site,
            "meta_key_user": meta_key_user,
        }
        conf["uservo"] = {"vo_mapping": vo_mapping}

        runtime = 55

        rec_1_values = {
            "rec_id": "test_record_1",
            "start_time": datetime(1984, 3, 3, 0, 0, 0),
            "stop_time": datetime(1985, 3, 3, 0, 0, 0),
            "n_cores": 8,
            "hepscore": 10.0,
            "tot_cpu": 15520000,
            "n_nodes": 1,
            "site": "test-site-1",
            "user": "first_user",
        }

        rec_2_values = {
            "rec_id": "test_record_2",
            "start_time": datetime(2023, 1, 1, 14, 24, 11),
            "stop_time": datetime(2023, 1, 2, 7, 11, 45),
            "n_cores": 1,
            "hepscore": 23.0,
            "tot_cpu": 12234325,
            "n_nodes": 2,
            "site": "test-site-2",
            "user": "second_user",
        }

        rec_value_list = [rec_1_values, rec_2_values]
        records = []

        with patch(
            "pyauditor.Record.runtime", new_callable=PropertyMock
        ) as mocked_runtime:
            mocked_runtime.return_value = runtime

            for r_values in rec_value_list:
                rec = create_rec(r_values, conf["auditor"])
                records.append(rec)

            conf["auditor"]["cpu_time_name"] = "fail"
            with pytest.raises(Exception) as pytest_error:
                create_summary_db(conf, records)
            assert pytest_error.type == KeyError

            conf["auditor"]["cpu_time_name"] = "TotalCPU"
            conf["auditor"]["nnodes_name"] = "fail"
            with pytest.raises(Exception) as pytest_error:
                create_summary_db(conf, records)
            assert pytest_error.type == KeyError

            conf["auditor"]["nnodes_name"] = "NNodes"
            conf["auditor"]["cores_name"] = "fail"
            with pytest.raises(Exception) as pytest_error:
                create_summary_db(conf, records)
            assert pytest_error.type == KeyError

            conf["auditor"]["cores_name"] = "Cores"
            conf["auditor"]["benchmark_name"] = "fail"
            with pytest.raises(Exception) as pytest_error:
                create_summary_db(conf, records)
            assert pytest_error.type == KeyError

            conf["auditor"]["benchmark_name"] = "HEPSPEC06"
            conf["site"][
                "site_name_mapping"
            ] = '{"test-site-2": "TEST_SITE_2"}'
            with pytest.raises(Exception) as pytest_error:
                create_summary_db(conf, records)
            assert pytest_error.type == KeyError

            conf["site"][
                "site_name_mapping"
            ] = '{"test-site-1": "TEST_SITE_1", "test-site-2": "TEST_SITE_2"}'
            conf["uservo"]["vo_mapping"] = (
                '{"^second": {"vo": "second_vo", "vogroup": "/second_vogroup",'
                ' "vorole": "Role=production"}}'
            )
            with pytest.raises(Exception) as pytest_error:
                create_summary_db(conf, records)
            assert pytest_error.type == KeyError
