import asyncio

from trinity.components.builtin.metrics.service.base import BaseMetricsService


class AsyncioMetricsService(BaseMetricsService):

    async def continuously_report(self) -> None:
        while self.manager.is_running:
            await super().report_now()
            await asyncio.sleep(self._reporting_frequency)
