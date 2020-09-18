from p2p import trio_utils
import asyncio

from trinity.components.builtin.metrics.service.base import BaseMetricsService


class TrioMetricsService(BaseMetricsService):

    async def continuously_report(self) -> None:
        # while self.manager.is_running:
            # await super().report_now()
            # await asyncio.sleep(self._reporting_frequency)
        async for _ in trio_utils.every(self._reporting_frequency):
            await super().report_now()
