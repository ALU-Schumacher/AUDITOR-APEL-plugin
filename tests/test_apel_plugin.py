import pytest
from apel_plugin import functions
from datetime import datetime
import pytz
import aiosqlite
import configparser


@pytest.mark.asyncio
class TestAPELPlugin:
    async def test_regex_dict_lookup(self):
        term_a = "apple"
        term_b = "banana"
        term_c = "citrus"

        dict = {"^a": "apple_in_dict", "^b": "banana_in_dict"}

        result = await functions.regex_dict_lookup(term_a, dict)
        assert result == "apple_in_dict"

        result = await functions.regex_dict_lookup(term_b, dict)
        assert result == "banana_in_dict"

        with pytest.raises(SystemExit) as pytest_error:
            await functions.regex_dict_lookup(term_c, dict)
        assert pytest_error.type == SystemExit
        assert pytest_error.value.code == 1

    async def test_get_begin_previous_month(self):
        time_a = datetime(2022, 10, 23, 12, 23, 55)
        time_b = datetime(1970, 1, 1, 00, 00, 00)

        result = await functions.get_begin_previous_month(time_a)
        assert result == datetime(2022, 9, 1, 00, 00, 00, tzinfo=pytz.utc)

        result = await functions.get_begin_previous_month(time_b)
        assert result == datetime(1969, 12, 1, 00, 00, 00, tzinfo=pytz.utc)

    async def test_sql_filter(self):
        create_table_sql = """
                           CREATE TABLE IF NOT EXISTS records(
                           site TEXT NOT NULL,
                           year INTEGER NOT NULL,
                           month INTEGER NOT NULL
                           )
                           """

        insert_record_sql = """
                            INSERT INTO records(
                               site,
                               year,
                               month
                           )
                           VALUES(
                               ?, ?, ?
                           )
                           """

        conn = await aiosqlite.connect(":memory:")
        cur = await conn.cursor()
        await cur.execute(create_table_sql)

        site_list = ["site_a", "site_b", "site_c"]
        year_list = [2020, 2021, 2022]
        month_list = [1, 7, 12]

        for site in site_list:
            for year in year_list:
                for month in month_list:
                    data_tuple = (site, year, month)
                    await cur.execute(insert_record_sql, data_tuple)
        await conn.commit()
        await cur.close()

        for site in site_list:
            for year in year_list:
                for month in month_list:
                    dst = await aiosqlite.connect(":memory:")
                    await conn.backup(dst)
                    filtered_db = await functions.sql_filter(
                        dst, month, year, site
                    )
                    cur = await filtered_db.cursor()
                    await cur.execute("SELECT * FROM records")
                    result = await cur.fetchall()
                    await cur.close()
                    await dst.close()

                    assert result == [(site, year, month)]

        await conn.close()

    async def test_create_time_db(self):
        path = ":memory:"
        conf = configparser.ConfigParser()

        publish_since_list = [
            "1970-01-01 00:00:00+00:00",
            "2020-01-01 17:23:00+00:00",
            "2022-12-17 20:20:20+01:00",
        ]

        for publish_since in publish_since_list:
            conf.read_string(f"[site]\npublish_since = {publish_since}")
            time_db = await functions.create_time_db(conf, path)
            cur = await time_db.cursor()
            await cur.execute("SELECT * FROM times")
            result = await cur.fetchall()
            await cur.close()
            await time_db.close()
            time_dt = datetime.strptime(publish_since, "%Y-%m-%d %H:%M:%S%z")
            time_stamp = time_dt.replace(tzinfo=pytz.utc).timestamp()

            assert result == [(time_stamp, datetime(1970, 1, 1, 0, 0, 0))]

    async def test_create_time_db_fail(self):
        path = "/home/nonexistent/time.db"
        conf = configparser.ConfigParser()

        publish_since = "1970-01-01 00:00:00+00:00"

        conf.read_string(f"[site]\npublish_since = {publish_since}")
        with pytest.raises(Exception) as pytest_error:
            await functions.create_time_db(conf, path)
        assert pytest_error.type == aiosqlite.OperationalError

        publish_since = "1970-01-01"

        conf.read_string(f"[site]\npublish_since = {publish_since}")
        with pytest.raises(Exception) as pytest_error:
            await functions.create_time_db(conf, path)
        assert pytest_error.type == ValueError
