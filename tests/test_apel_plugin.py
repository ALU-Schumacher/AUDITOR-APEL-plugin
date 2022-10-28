import pytest
from apel_plugin import functions


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
