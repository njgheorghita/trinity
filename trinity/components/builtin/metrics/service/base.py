import time
import aiohttp
import asks
from typing import Dict
import base64
from abc import abstractmethod
from urllib import parse
from http.client import HTTPException

from async_service import Service
from pyformance.reporters.influx import InfluxReporter

from trinity.components.builtin.metrics.abc import MetricsServiceAPI
from trinity.components.builtin.metrics.registry import HostMetricsRegistry
from trinity._utils.logging import get_logger


class ExtendedInfluxReporter(InfluxReporter):
    """
    ``InfluxReporter`` extended to enable sending annotations to InfluxDB
    """
    async def send_annotation(self, annotation_data: str) -> None:
        url = parse.urlunparse((
            self.protocol,  # scheme
            f"{self.server}:{self.port}",  # netloc
            "/write",  # path
            '',
            f"db={self.database}&precision=s",  # query
            ''
        ))
        await self._post(url, annotation_data)

    def _generate_auth_header(self) -> Dict[str, str]:
        auth_string = ("%s:%s" % (self.username, self.password)).encode()
        auth = base64.b64encode(auth_string)
        return {"Authorization": "Basic %s" % auth.decode("utf-8")}

    async def _post(self, url, data):
        # what if no auth_header / self.username?
        # should we catch errors? copy strategy from other place in code
        # auth_header = self._generate_auth_header()
        # if auth_header:
            # async with aiohttp.ClientSession() as session:
                # await session.post(url, data=data, headers=auth_header)
        # else:
        # async with aiohttp.ClientSession() as session:
        await asks.post(url, data=data)


    # use instead of pyformance.reporters.InfluxReporter.report_now
    async def report_async(self, registry=None, timestamp=None):
        print("REPORT_ASYNC")
        timestamp = timestamp or int(round(self.clock.time()))
        metrics = (registry or self.registry).dump_metrics()
        post_data = []
        for key, metric_values in metrics.items():
            if not self.prefix:
                table = key
            else:
                table = "%s.%s" % (self.prefix, key)
            values = ",".join(["%s=%s" % (k, v if type(v) is not str \
                                               else '"{}"'.format(v))
                              for (k, v) in metric_values.items()])
            line = "%s %s %s" % (table, values, timestamp)
            post_data.append(line)
        post_data = "\n".join(post_data)
        path = "/write?db=%s&precision=s" % self.database
        url = "%s://%s:%s%s" % (self.protocol, self.server, self.port, path)
        await self._post(url, post_data)


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
        which can report metrics and send annotations to connected InfluxDB.
        """
        return self._reporter

    async def run(self) -> None:
        self.logger.info("Reporting metrics to %s", self._influx_server)
        self.manager.run_daemon_task(self.continuously_report)
        await self.manager.wait_finished()

    async def report_now(self) -> None:
        try:
            print("REPORT_NOW")
            await self._reporter.report_async()
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
