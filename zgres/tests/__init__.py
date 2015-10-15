import asyncio
from asyncio import sleep

class FakeSleeper:

    def __init__(self, max_loops=20):
        self.log = []
        self.max_loops = max_loops
        self.finished = asyncio.Event()
        self._next = asyncio.Event()
        self.wait = self.finished.wait
        asyncio.get_event_loop().call_later(10, self._watchdog)

    def _watchdog(self):
        self.finished.set()
        self._next.set()
        raise Exception('Timed Out')

    def _check_finished(self):
        if self.finished.is_set():
            raise AssertionError('Already finished')

    async def __call__(self, delay):
        self.log.append(delay)
        self._next.set()
        await sleep(0)
        if self.max_loops is not None and len(self.log) >= self.max_loops:
            self.finished.set()
            await sleep(1)
        self._check_finished()

    async def next(self):
        # wait till the next __call__
        await self._next.wait()
        self._next.clear()
        self._check_finished()
