import pytest
import apel_plugin.apel_plugin as apel_plugin


class TestAPELPlugin(object):
    @pytest.mark.asyncio
    async def test_regex_dict_lookup(self):
        term_a = "apple"
        term_b = "banana"
        dict = {"^a": "apple_in_dict", "^b": "banana_in_dict"}

        result = await apel_plugin.regex_dict_lookup(term_a, dict)
        assert result == "apple_in_dict"

        result = await apel_plugin.regex_dict_lookup(term_b, dict)
        assert result == "banana_in_dict"
