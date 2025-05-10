from pytest import approx

from quantari.indicators import EMA


def test_ema_basic():
    ema = EMA(period=3)

    # Calculate with different values accumulated
    assert ema.calculate({"close": 10}) == approx(5.0)
    assert ema.calculate({"close": 12}) == approx(8.5)

    # None if no close
    assert ema.calculate({}) is None

    # __str__
    assert str(ema) == "EMA_3"
