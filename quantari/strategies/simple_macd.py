from .signals import Signals


# TODO: We sould keep the current state so instead of reporting the same signal BUY/SELL multiple times,
# we just signal once and then using HOLD until a change is required
class SimpleMACD:
    def __init__(self, name=None):
        self.name = name if name else "Simple_MACD"
        self.last_signal = Signals.HOLD

    def evaluate(self, message: dict) -> list[float]:
        indicators = message["indicators"]
        macd, signal = indicators.get("MACD_12_26_9", [None, None])

        if not macd or not signal or macd == signal:
            return Signals.HOLD

        new_signal = Signals.BUY if macd > signal else Signals.SELL

        if new_signal != self.last_signal:
            self.last_signal = new_signal
            return new_signal

        return Signals.HOLD

    def __str__(self):
        return self.name
