from abc import abstractmethod

from async_service import ServiceAPI
from pyformance import MetricsRegistry
from pyformance.reporters import InfluxReporter


class MetricsServiceAPI(ServiceAPI):

    @abstractmethod
    def __init__(self,
                 influx_server: str,
                 influx_user: str,
                 influx_password: str,
                 influx_database: str,
                 host: str,
                 port: int,
                 protocol: str,
                 reporting_frequency: int) -> None:
        ...

    @property
    @abstractmethod
    def registry(self) -> MetricsRegistry:
        ...

    @property
    @abstractmethod
    def reporter(self) -> InfluxReporter:
        ...
