from dataclasses import dataclass


@dataclass
class Signals:
    HOLD: int = 0
    BUY: int = 1
    SELL: int = -1
