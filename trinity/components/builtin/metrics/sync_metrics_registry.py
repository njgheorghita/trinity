import time
from pyformance import MetricsRegistry
from pyformance.reporters import InfluxReporter

from lahja import BaseEvent, EndpointAPI


class PivotEvent(BaseEvent):
    def __init__(self, payload: int) -> None:
        super().__init__()
        self.payload = payload


class SyncMetricsRegistry:
    """
    Registry to track weighted moving average of pivot events, and report to InfluxDB.
    """
    def __init__(self,
                 metrics_registry: MetricsRegistry,
                 metrics_reporter: InfluxReporter,
                 event_bus: EndpointAPI
                 ) -> None:
        self.host = metrics_registry.host
        self.pivot_meter = metrics_registry.meter('trinity.p2p/sync/pivot_rate.meter')
        self.reporter = metrics_reporter
        self.event_bus = event_bus

    def subscribe_to_pivot_events(self) -> None:
        self.event_bus.subscribe(PivotEvent, lambda event: self.record_pivot(event.payload))

    def record_pivot(self, block_number: int) -> None:
        # record pivot and send event annotation to influxdb
        self.pivot_meter.mark()
        pivot_time = int(time.time())
        post_data = (
            f'events title="beam pivot @ block {block_number}",'
            f'text="pivot event",tags="{self.host}" {pivot_time}'
        )
        self.reporter.send_annotation(post_data)
