from .ema import EMA


class MACD:
    def __init__(
        self, fast: int = 12, slow: int = 26, signal: int = 9, name: str = None
    ):
        self.fast = fast
        self.slow = slow
        self.signal = signal

        self.fast_ema = EMA(fast)
        self.slow_ema = EMA(slow)
        self.signal_ema = EMA(signal)

        self.name = name if name else f"MACD_{fast}_{slow}_{signal}"

    def calculate(self, message: dict) -> list[float]:
        close = message.get("close")

        if close is None:
            return None

        fast_ema_value = self.fast_ema.calculate(message)
        slow_ema_value = self.slow_ema.calculate(message)

        if fast_ema_value is None or slow_ema_value is None:
            return None

        macd = fast_ema_value - slow_ema_value
        signal = self.signal_ema.calculate({"close": macd})

        return [macd, signal]

    def __str__(self):
        return self.name
