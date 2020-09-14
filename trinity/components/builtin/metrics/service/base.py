import time
from typing import Dict
import requests
import base64
from abc import abstractmethod
from http.client import HTTPException

from async_service import Service
from pyformance.reporters.influx import InfluxReporter
from lahja import EndpointAPI

from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.components.builtin.metrics.registry import HostMetricsRegistry
from trinity.components.builtin.metrics.sync_metrics_registry import SyncMetricsRegistry
from trinity._utils.logging import get_logger


class ExtendedInfluxReporter(InfluxReporter):
    """
    `InfluxReporter` extended to enable sending annotations to InfluxDB
    """
    def send_annotation(self, annotation_data: str) -> None:
        path = f"/write?db={self.database}&precision=s"
        url = f"{self.protocol}://{self.server}:{self.port}{path}"
        auth_header = self._generate_auth_header()
        requests.post(url, data=annotation_data, headers=auth_header)

    def _generate_auth_header(self) -> Dict[str, str]:
        auth_string = ("%s:%s" % (self.username, self.password)).encode()
        auth = base64.b64encode(auth_string)
        return {"Authorization": "Basic %s" % auth.decode("utf-8")}


class BaseMetricsService(Service, MetricsServiceAPI):
    """
    A service to provide a registry where metrics instruments can be registered and retrieved from.
    It continuously reports metrics to the specified InfluxDB instance.
    """

    MIN_SECONDS_BETWEEN_ERROR_LOGS = 60

    def __init__(self,
                 influx_server: str,
                 influx_user: str,
                 influx_password: str,
                 influx_database: str,
                 host: str,
                 port: int,
                 protocol: str,
                 reporting_frequency: int):
        self._unreported_error: Exception = None
        self._last_time_reported: float = 0.0
        self._influx_server = influx_server
        self._reporting_frequency = reporting_frequency
        self._registry = HostMetricsRegistry(host)
        self._reporter = ExtendedInfluxReporter(
            registry=self._registry,
            database=influx_database,
            username=influx_user,
            password=influx_password,
            protocol=protocol,
            port=port,
            server=influx_server,
        )

    logger = get_logger('trinity.components.builtin.metrics.MetricsService')
    sync_metrics_registry = None

    @property
    def registry(self) -> HostMetricsRegistry:
        """
        Return the :class:`trinity.components.builtin.metrics.registry.HostMetricsRegistry` at which
        metrics instruments can be registered and retrieved.
        """
        return self._registry

    @property
    def reporter(self) -> ExtendedInfluxReporter:
        """
        Return the :class:`trinity.components.builtin.metrics.service.base.ExtendedInfluxReporter`
        with influxdb connection details and annotations supported.
        """
        return self._reporter

    def subscribe_to_pivot_events(self, event_bus: EndpointAPI) -> None:
        """
        Subscribe to beam sync pivot events triggered by the syncing service.
        """
        self.sync_metrics_registry = SyncMetricsRegistry(self.registry, self.reporter, event_bus)
        self.sync_metrics_registry.subscribe_to_pivot_events()

    async def run(self) -> None:
        self.logger.info("Reporting metrics to %s", self._influx_server)
        self.manager.run_daemon_task(self.continuously_report)
        await self.manager.wait_finished()

    def report_now(self) -> None:
        try:
            self._reporter.report_now()
        except (HTTPException, ConnectionError) as exc:

            # This method is usually called every few seconds. If there's an issue with the
            # connection we do not want to flood the log and tame down warnings.

            # 1. We log the first instance of an exception immediately
            # 2. We log follow up exceptions only after a minimum time has elapsed
            # This means that we also might overwrite exceptions for different errors

            if self._is_justified_to_log_error():
                self._log_and_clear(exc)
            else:
                self._unreported_error = exc
        else:
            # If errors disappear, we want to make sure we eventually report the last instance
            if self._unreported_error is not None and self._is_justified_to_log_error():
                self._log_and_clear(self._unreported_error)

    def _log_and_clear(self, error: Exception) -> None:
        self.logger.warning("Unable to report metrics: %s", error)
        self._unreported_error = None
        self._last_time_reported = time.monotonic()

    def _is_justified_to_log_error(self) -> bool:
        return (
            self._last_time_reported == 0.0 or
            time.monotonic() - self._last_time_reported > self.MIN_SECONDS_BETWEEN_ERROR_LOGS
        )

    @abstractmethod
    async def continuously_report(self) -> None:
        ...
