import pytest

from quantari.decorators.catch_and_set_exception import catch_and_set_exception


class Dummy:
    def __init__(self):
        self.exception = False

    @catch_and_set_exception
    async def will_raise(self):
        raise ValueError("Test error")

    @catch_and_set_exception
    async def will_not_raise(self):
        return "ok"


@pytest.mark.asyncio
async def test_catch_and_set_exception_sets_flag_on_exception():
    d = Dummy()
    await d.will_raise()
    assert d.exception is True


@pytest.mark.asyncio
async def test_catch_and_set_exception_no_flag_on_success():
    d = Dummy()
    result = await d.will_not_raise()
    assert d.exception is False
    assert result == "ok"
