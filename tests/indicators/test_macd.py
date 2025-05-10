from pytest import approx

from quantari.indicators import MACD


def test_macd():
    macd = MACD(fast=2, slow=3, signal=2)

    # Calculate macd with one value
    result = macd.calculate({"close": 10})

    assert isinstance(result, list)
    assert result[0] == approx(1.66, rel=1e-2)
    assert result[1] == approx(1.11, rel=1e-2)

    # None if no close
    assert macd.calculate({}) is None

    # __str__
    assert str(macd) == "MACD_2_3_2"
