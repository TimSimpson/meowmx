class BackoffCalc:
    def __init__(self, min_value: int, max_value: int) -> None:
        self._min = min_value
        self._max = max_value
        self._current = self._min

    def failure(self) -> int:
        """Returns time to wait."""
        wait = self._current
        self._current = min(self._current * 2, self._max)
        return wait

    def success(self) -> None:
        """Resets the wait time."""
        self._current = self._min
