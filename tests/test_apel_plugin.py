import pytest
from apel_plugin import (
    regex_dict_lookup,
    get_begin_previous_month,
    create_time_db,
    get_time_db,
)
from datetime import datetime
import pytz
import aiosqlite
import os


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
