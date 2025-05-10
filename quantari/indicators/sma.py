class SMA:
    def __init__(self, period: int = 50, name: str = None):
        self.period = period
        self.data = []
        self.name = name if name else f"SMA_{self.period}"
        self.sum = 0

    def calculate(self, message: dict) -> float:
        close = message.get("close")

        if close is None:
            return None

        self.data.append(close)
        self.sum += close

        if len(self.data) > self.period:
            self.sum -= self.data.pop(0)

        if len(self.data) < self.period:
            return None

        return self.sum / self.period

    def __str__(self):
        return self.name
