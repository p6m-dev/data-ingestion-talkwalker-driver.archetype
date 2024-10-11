class Metric:
    """Metric is a wrapper class for integer counters that can be reported to Cloud Metrics Service"""

    def __init__(self, name: str, value: int = 0):
        self.name = name
        self.value = value

    def increment(self, value: int = 1):
        self.value += value

    def decrement(self, value: int = 1):
        self.value -= value

    def get(self) -> int:
        return self.value