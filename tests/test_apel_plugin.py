import pytest
from apel_plugin import functions
from datetime import datetime
import pytz


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
