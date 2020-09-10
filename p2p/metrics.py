import time
import base64
from typing import Dict, Generic
from pyformance import MetricsRegistry
from pyformance.reporters import InfluxReporter

import requests
from lahja import BaseEvent, EndpointAPI

from p2p.abc import TPeer
from p2p.exceptions import PeerReporterRegistryError


class PeerReporterRegistry(Generic[TPeer]):
    """
    Registry to track active peers and report metrics to influxdb/grafana.
    """
    def __init__(self, metrics_registry: MetricsRegistry) -> None:
        self.metrics_registry = metrics_registry
        self._peer_reporters: Dict[TPeer, int] = {}

    def assign_peer_reporter(self, peer: TPeer) -> None:
        if peer in self._peer_reporters.keys():
            raise PeerReporterRegistryError(
                f"Cannot reassign peer: {peer} to PeerReporterRegistry. "
                "Peer already assigned."
            )
        min_unclaimed_id = self._get_min_unclaimed_gauge_id()
        self._peer_reporters[peer] = min_unclaimed_id
        self.reset_peer_meters(min_unclaimed_id)

    def unassign_peer_reporter(self, peer: TPeer) -> None:
        try:
            gauge_id = self._peer_reporters[peer]
        except AttributeError:
            raise PeerReporterRegistryError(
                f"Cannot unassign peer: {peer} to PeerReporterRegistry. "
                "Peer not found in peer reporter registry."
            )
        self.reset_peer_meters(gauge_id)
        del self._peer_reporters[peer]

    def _get_min_unclaimed_gauge_id(self) -> int:
        try:
            max_claimed_id = max(self._peer_reporters.values())
        except ValueError:
            return 0

        unclaimed_ids = [
            num for num in range(0, max_claimed_id + 1) if num not in self._peer_reporters.values()
        ]
        if len(unclaimed_ids) == 0:
            return max_claimed_id + 1

        return min(unclaimed_ids)

    def trigger_peer_reports(self) -> None:
        for peer, peer_id in self._peer_reporters.items():
            if peer.get_manager().is_running:
                self.make_periodic_update(peer, peer_id)

    def reset_peer_meters(self, peer_id: int) -> None:
        # subclasses are responsible for implementing method that resets all implemented meters
        pass

    def make_periodic_update(self, peer: TPeer, peer_id: int) -> None:
        # subclasses are responsible for implementing method that updates all implemented meters
        pass


class PivotEvent(BaseEvent):
    def __init__(self, payload):
        super().__init__()
        self.payload = payload


class SyncMetricsRegistry:
    """
    Track pivot rate and create annotations for influxdb.
    """
    def __init__(self,
                 metrics_registry: MetricsRegistry,
                 metrics_reporter: InfluxReporter,
                 event_bus: EndpointAPI
                 ) -> None:
        self.protocol = metrics_reporter.protocol
        self.server = metrics_reporter.server
        self.port = metrics_reporter.port
        self.database = metrics_reporter.database
        self.username = metrics_reporter.username
        self.password = metrics_reporter.password
        self.host = metrics_registry.host
        self.event_bus = event_bus
        self.event_bus.subscribe(PivotEvent, lambda event: self.record_pivot(event.payload))
        self.pivot_gauge = metrics_registry.gauge('trinity.p2p/sync/pivot_rate.gauge')
        self.pivot_gauge.set_value(0)
        self.pivot_timestamps = []

    def trigger_sync_reports(self) -> None:
        # update influxdb pivots/hour gauge
        pivots_over_last_hour = self.get_pivots_over_last_hour()
        self.pivot_gauge.set_value(pivots_over_last_hour)

    def record_pivot(self, block_number: int) -> None:
        # record pivot and send event annotation to influxdb
        current_time = int(time.time())
        self.pivot_timestamps.append(current_time)
        self._post_pivot_annotations(current_time, block_number)
        pivots_over_last_hour = self.get_pivots_over_last_hour(current_time)
        self.pivot_gauge.set_value(pivots_over_last_hour)

    def get_pivots_over_last_hour(self, current_time=None):
        if not current_time:
            current_time = int(time.time())
        return len([stamp for stamp in self.pivot_timestamps if stamp > current_time - 3600])

    def _post_pivot_annotations(self, pivot_time: int, block_number: int) -> None:
        path = f"/write?db={self.database}&precision=s"
        url = f"{self.protocol}://{self.server}:{self.port}{path}"
        auth_header = self._generate_auth_header()
        post_data = (
            f'events title="beam pivot @ block {block_number}",'
            f'text="pivot event",tags="{self.host}" {pivot_time}'
        )
        requests.post(url, data=post_data, headers=auth_header)

    def _generate_auth_header(self) -> Dict[str, str]:
        auth_string = ("%s:%s" % (self.username, self.password)).encode()
        auth = base64.b64encode(auth_string)
        return {"Authorization": "Basic %s" % auth.decode("utf-8")}
