class EMA:
    def __init__(self, period: int = 50, name: str = None):
        self.period = period
        self.alpha = 2 / (self.period + 1)
        self.last_ema = 0
        self.name = name if name else f"EMA_{self.period}"

    def calculate(self, message: dict) -> float:
        close = message.get("close")

        if close is None:
            return None

        self.last_ema = (close * self.alpha) + (self.last_ema * (1 - self.alpha))

        return self.last_ema

    def __str__(self):
        return self.name
