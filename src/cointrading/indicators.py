from __future__ import annotations

import math


def sma(values: list[float], window: int) -> float | None:
    if window <= 0:
        raise ValueError("window must be positive")
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def returns(values: list[float]) -> list[float]:
    output: list[float] = []
    for prev, current in zip(values, values[1:]):
        if prev == 0:
            output.append(0.0)
        else:
            output.append((current / prev) - 1.0)
    return output


def realized_volatility(values: list[float], window: int) -> float | None:
    rets = returns(values)
    if len(rets) < window:
        return None
    sample = rets[-window:]
    mean = sum(sample) / len(sample)
    variance = sum((item - mean) ** 2 for item in sample) / len(sample)
    return math.sqrt(variance)
