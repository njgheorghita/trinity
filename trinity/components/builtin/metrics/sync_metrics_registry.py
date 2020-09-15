import time
from pyformance import MetricsRegistry
from pyformance.reporters import InfluxReporter

from eth_typing import BlockNumber


class SyncMetricsRegistry:
    """
    Registry to track weighted moving average of pivot events, and report to InfluxDB.
    """
    def __init__(self,
                 metrics_registry: MetricsRegistry,
                 metrics_reporter: InfluxReporter,
                 ) -> None:
        self.host = metrics_registry.host
        self.pivot_meter = metrics_registry.meter('trinity.p2p/sync/pivot_rate.meter')
        self.reporter = metrics_reporter

    async def record_pivot(self, block_number: BlockNumber) -> None:
        # record pivot and send event annotation to influxdb
        self.pivot_meter.mark()
        pivot_time = int(time.time())
        post_data = (
            f'events title="beam pivot @ block {block_number}",'
            f'text="pivot event",tags="{self.host}" {pivot_time}'
        )
        await self.reporter.send_annotation(post_data)
