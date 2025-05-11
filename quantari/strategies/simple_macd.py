from .signals import Signals


class SimpleMACD:
    def __init__(self, name=None):
        self.name = name if name else "Simple_MACD"

    def evaluate(self, message: dict) -> list[float]:
        indicators = message["indicators"]
        macd, signal = indicators.get("MACD_12_26_9", [None, None])

        if not macd or not signal or macd == signal:
            return Signals.HOLD

        return Signals.BUY if macd > signal else Signals.SELL

    def __str__(self):
        return self.name
