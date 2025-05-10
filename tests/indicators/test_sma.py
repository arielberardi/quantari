from pytest import approx

from quantari.indicators import SMA


def test_sma_basic():
    sma = SMA(period=3)

    # Not enough data yet
    assert sma.calculate({"close": 10}) is None
    assert sma.calculate({"close": 12}) is None

    # Third value, should return average
    assert sma.calculate({"close": 14}) == approx((10 + 12 + 14) / 3)

    # Next value, should drop the oldest
    assert sma.calculate({"close": 16}) == approx((12 + 14 + 16) / 3)

    # None if no close
    assert sma.calculate({}) is None

    # __str__
    assert str(sma) == "SMA_3"
