from quantari.strategies import Signals, SimpleMACD


def test_strategy_name():
    simpleMACD = SimpleMACD(name="Strategy")
    assert str(simpleMACD) == "Strategy"


def test_invalid_values():
    simpleMACD = SimpleMACD()

    message = {"indicators": {"MACD_12_26_9": [None, None]}}
    assert simpleMACD.evaluate(message) is Signals.HOLD

    message = {"indicators": {"MACD_12_26_9": [None, 1]}}
    assert simpleMACD.evaluate(message) is Signals.HOLD

    message = {"indicators": {"MACD_12_26_9": [1, None]}}
    assert simpleMACD.evaluate(message) is Signals.HOLD

    message = {"indicators": {"MACD_12_26_9": [1, 1]}}
    assert simpleMACD.evaluate(message) is Signals.HOLD


def test_bullish_crossover():
    message = {"indicators": {"MACD_12_26_9": [10, 4]}}
    simpleMACD = SimpleMACD()
    assert simpleMACD.evaluate(message) is Signals.BUY


def test_bearish_crossover():
    message = {"indicators": {"MACD_12_26_9": [4, 10]}}
    simpleMACD = SimpleMACD()
    assert simpleMACD.evaluate(message) is Signals.SELL
