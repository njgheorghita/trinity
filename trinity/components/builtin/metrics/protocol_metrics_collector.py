from typing import NamedTuple
from async_service import (
    as_service,
    ManagerAPI,
)
from p2p import trio_utils
from trinity.boot_info import BootInfo
from lahja import EndpointAPI
# from trinity.protocol.eth.peer import EthPeerPoolEventServer
from trinity.nodes.full import FullNode


class Eth65Stats(NamedTuple):
    tx_broadcast_65_value: int
    tx_announce_65_value: int


def read_protocol_stats(event_server):
    return Eth65Stats(tx_broadcast_65_value=100, tx_announce_65_value=10)


@as_service
async def collect_protocol_metrics(manager: ManagerAPI,
                                   boot_info: BootInfo,
                                   event_bus: EndpointAPI,
                                   metrics_service,
                                   frequency_seconds: int) -> None:
    previous = None # type

    tx_broadcast_65_gauge = metrics_service.registry.gauge('trinity.protocol/tx.braodcast.65/rate.gauge')
    tx_announce_65_gauge = metrics_service.registry.gauge('trinity.protocol/tx.announce.65/rate.gauge')
    # tx_braodcast_64_gauge = registry.gauge('trinity.protocol/tx.braodcast.64/rate.gauge')
    # tx_announce_64_gauge = registry.gauge('trinity.protocol/tx.announce.64/rate.gauge')
    node = FullNode(event_bus, metrics_service, boot_info.trinity_config)
    event_server = node.get_event_server()
    
    async for _ in  trio_utils.every(frequency_seconds):
        current = read_protocol_stats(event_server)
        tx_broadcast_65_gauge.set_value(current.tx_broadcast_65_value)
        tx_announce_65_gauge.set_value(current.tx_announce_65_value)
