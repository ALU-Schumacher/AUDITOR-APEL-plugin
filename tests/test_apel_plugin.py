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
)
from datetime import datetime
import pytz
import aiosqlite
import os
import subprocess


@pytest.mark.asyncio
class TestAPELPlugin:
    async def test_regex_dict_lookup(self):
        term_a = "apple"
        term_b = "banana"
        term_c = "citrus"

        dict = {"^a": "apple_in_dict", "^b": "banana_in_dict"}

        result = await regex_dict_lookup(term_a, dict)
        assert result == "apple_in_dict"

        result = await regex_dict_lookup(term_b, dict)
        assert result == "banana_in_dict"

        result = await regex_dict_lookup(term_c, dict)
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
