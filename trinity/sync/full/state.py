import asyncio
import collections
import functools
import itertools
import logging
from pathlib import Path
import tempfile
import time
from typing import (
    Any,
    Awaitable,
    cast,
    Dict,
    Iterable,
    List,
    Set,
    FrozenSet,
    Tuple,
    Type,
)

import eth_utils.toolz

import rlp

from eth_hash.auto import keccak
from eth_utils import (
    decode_hex,
    encode_hex,
    ValidationError,
    remove_0x_prefix,
)

from eth_typing import (
    Hash32
)

from cancel_token import CancelToken

from eth.constants import (
    BLANK_ROOT_HASH,
    EMPTY_SHA3,
)
from eth.db.backends.level import LevelDB
from eth.db.backends.base import BaseDB
from eth.rlp.accounts import Account
from eth.tools.logging import ExtendedDebugLogger

from trie.constants import (
    NODE_TYPE_BLANK,
    NODE_TYPE_BRANCH,
    NODE_TYPE_EXTENSION,
    NODE_TYPE_LEAF,
    BLANK_NODE,
)
from trie.utils.nodes import (
    decode_node,
    get_node_type,
    is_blank_node,
)

from p2p.service import BaseService
from p2p.protocol import (
    Command,
)

from p2p.exceptions import (
    NoEligiblePeers,
    NoIdlePeers,
    PeerConnectionLost,
)
from p2p.peer import BasePeer, PeerSubscriber

from trinity.db.base import AsyncBaseDB
from trinity.db.chain import AsyncChainDB
from trinity.exceptions import (
    AlreadyWaiting,
    SyncRequestAlreadyProcessed,
)
from trinity.protocol.eth.peer import ETHPeer, ETHPeerPool
from trinity.protocol.eth.monitors import (
    ETHChainTipMonitor,
    ETHVerifiedTipMonitor,
)
from trinity.protocol.eth import (
    constants as eth_constants,
)
from trinity.sync.full.hexary_trie import (
    HexaryTrieSync,
    SyncRequest,
)
from trinity._utils.os import get_open_fd_limit
from trinity._utils.timer import Timer


class StateDownloader(BaseService, PeerSubscriber):
    _total_processed_nodes = 0
    _report_interval = 10  # Number of seconds between progress reports.
    _reply_timeout = 20  # seconds
    _timer = Timer(auto_start=False)
    _total_timeouts = 0

    def __init__(self,
                 chaindb: AsyncChainDB,
                 account_db: AsyncBaseDB,
                 peer_pool: ETHPeerPool,
                 token: CancelToken = None) -> None:
        super().__init__(token)
        self.chaindb = chaindb
        self.peer_pool = peer_pool
        self._account_db = account_db

        # We use a LevelDB instance for the nodes cache because a full state download, if run
        # uninterrupted will visit more than 180M nodes, making an in-memory cache unfeasible.
        self._nodes_cache_dir = tempfile.TemporaryDirectory(prefix="pyevm-state-sync-cache")

        self.request_tracker = TrieNodeRequestTracker(self._reply_timeout, self.logger)
        self._peer_missing_nodes: Dict[ETHPeer, Set[Hash32]] = collections.defaultdict(set)

        # will be fired once the correct state tree to crawl has been determined
        self._ready = asyncio.Event()
        self.scheduler = None

        self.packet_count = 0

    # We are only interested in peers leaving the pool
    subscription_msg_types: FrozenSet[Type[Command]] = frozenset()

    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize: int = 2000

    def deregister_peer(self, peer: BasePeer) -> None:
        # Use .pop() with a default value as it's possible we never requested anything to this
        # peer or it had all the trie nodes we requested, so there'd be no entry in
        # self._peer_missing_nodes for it.
        self._peer_missing_nodes.pop(cast(ETHPeer, peer), None)

    async def get_peer_for_request(self, node_keys: Set[Hash32]) -> ETHPeer:
        """Return an idle peer that may have any of the trie nodes in node_keys.

        If none of our peers have any of the given node keys, raise NoEligiblePeers. If none of
        the peers which may have at least one of the given node keys is idle, raise NoIdlePeers.
        """
        has_eligible_peers = False
        async for peer in self.peer_pool:
            peer = cast(ETHPeer, peer)
#            if self._peer_missing_nodes[peer].issuperset(node_keys):
#                self.logger.debug2("%s doesn't have any of the nodes we want, skipping it", peer)
#                continue
            has_eligible_peers = True
            if peer in self.request_tracker.active_requests:
                self.logger.debug2("%s is not idle, skipping it", peer)
                continue
            return peer

        if not has_eligible_peers:
            raise NoEligiblePeers()
        else:
            raise NoIdlePeers()

    async def _process_nodes(self, nodes: Iterable[Tuple[Hash32, bytes]]) -> None:
        for node_key, node in nodes:
            self._total_processed_nodes += 1
            try:
                await self.scheduler.process([(node_key, node)])
            except SyncRequestAlreadyProcessed:
                # This means we received a node more than once, which can happen when we
                # retry after a timeout.
                pass

    async def _cleanup(self) -> None:
        self._nodes_cache_dir.cleanup()

    async def request_nodes(self, node_keys: Iterable[Hash32]) -> None:
        not_yet_requested = set(node_keys)
        while not_yet_requested:
            try:
                peer = await self.get_peer_for_request(not_yet_requested)
            except NoIdlePeers:
                self.logger.debug2(
                    "No idle peers have any of the %d trie nodes we want, sleeping a bit",
                    len(not_yet_requested),
                )
                await self.sleep(0.2)
                continue
            except NoEligiblePeers:
                self.request_tracker.missing[time.time()] = list(not_yet_requested)
                self.logger.debug(
                    "No peers have any of the %d trie nodes in this batch, will retry later",
                    len(not_yet_requested),
                )
                # TODO: disconnect a peer if the pool is full
                return

#           candidates = list(not_yet_requested.difference(self._peer_missing_nodes[peer]))
            candidates = list(not_yet_requested)
            batch = tuple(candidates[:eth_constants.MAX_STATE_FETCH])
            not_yet_requested = not_yet_requested.difference(batch)
            self.request_tracker.active_requests[peer] = (time.time(), batch)
            self.run_task(self._request_and_process_nodes(peer, batch))

    async def _request_and_process_nodes(self, peer: ETHPeer, batch: Tuple[Hash32, ...]) -> None:
        self.logger.debug("Requesting %d trie nodes from %s", len(batch), peer)
        try:
            if any(item == b'' for item in batch):
                self.logger.debug('Submitting batch with empty item')
            self.packet_count += 1
            node_data = await peer.requests.get_node_data(batch)
        except TimeoutError as err:
            self.logger.debug(
                "Timed out waiting for %s trie nodes from %s: %s",
                len(batch),
                peer,
                err,
            )
            node_data = tuple()
        except AlreadyWaiting as err:
            self.logger.warning(
                "Already waiting for a NodeData response from %s", peer,
            )
            return

        try:
            self.request_tracker.active_requests.pop(peer)
        except KeyError:
            self.logger.warning("Unexpected error removing peer from active requests: %s", peer)

        self.logger.debug("Got %d NodeData entries from %s", len(node_data), peer)

        if node_data:
            node_keys, _ = zip(*node_data)
        else:
            node_keys = tuple()

        # check for missing nodes and re-schedule them
        missing = set(batch).difference(node_keys)

        # TODO: this doesn't necessarily mean the peer doesn't have them, just
        # that they didn't respond with them this time.  We should explore
        # alternate ways to do this since a false negative here will result in
        # not requesting this node from this peer again.
        if missing:
            self._peer_missing_nodes[peer].update(missing)
            self.logger.debug(
                "Re-requesting %d/%d NodeData entries not returned by %s",
                len(missing),
                len(batch),
                peer,
            )
            await self.request_nodes(missing)

        if node_data:
            await self._process_nodes(node_data)

    async def _periodically_retry_timedout_and_missing(self) -> None:
        while self.is_operational:
            timed_out = self.request_tracker.get_timed_out()
            if timed_out:
                self.logger.debug("Re-requesting %d timed out trie nodes", len(timed_out))
                self._total_timeouts += len(timed_out)
                await self.request_nodes(timed_out)

            retriable_missing = self.request_tracker.get_retriable_missing()
            if retriable_missing:
                self.logger.debug("Re-requesting %d missing trie nodes", len(retriable_missing))
                await self.request_nodes(retriable_missing)

            # Finally, sleep until the time either our oldest request is scheduled to timeout or
            # one of our missing batches is scheduled to be retried.
            next_timeout = self.request_tracker.get_next_timeout()
            await self.sleep(next_timeout - time.time())

    async def _monitor_new_blocks(self) -> None:
        monitor = ETHVerifiedTipMonitor(self.peer_pool, self.cancel_token)
        self.run_daemon(monitor)
        first_root, second_root = None, None
        async for (peer, header) in monitor.wait_tip_info():
            root_hash = header.state_root

            if not first_root:
                first_root = root_hash
            else:
                second_root = root_hash
                break

        self.logger.debug(
            'diffing trees rooted at %s and %s',
            encode_hex(first_root), encode_hex(second_root)
        )

        # Allow the LevelDB instance to consume half of the entire file descriptor limit that
        # the OS permits. Let the other half be reserved for other db access, networking etc.
        max_open_files = get_open_fd_limit() // 2
        self.scheduler = TreeDiffStateSync(
            first_root, second_root,
            LevelDB(Path(self._nodes_cache_dir.name), max_open_files),
            self.logger,
        )

        self._ready.set()  # we can start syncing!

    async def _run(self) -> None:
        """Fetch all trie nodes starting from self.root_hash, and store them in self.db.

        Raises OperationCancelled if we're interrupted before that is completed.
        """
        self.logger.info("Waiting for a client to connect")
        self.run_task(self._monitor_new_blocks())

        await self.wait(self._ready.wait())

        self._timer.start()
        self.run_task(self._periodically_report_progress())
        self.run_task(self._periodically_retry_timedout_and_missing())
        with self.subscribe(self.peer_pool):
            while self.scheduler.has_pending_requests:
                # This ensures we yield control and give deregister_peer a chance to run.
                # It also ensures we exit when our cancel token is triggered, and that
                # _monitor_new_blocks has a chance to change the root_hash
                await self.sleep(0)

                requests = self.scheduler.next_batch(eth_constants.MAX_STATE_FETCH)
                if not requests:
                    # Although we frequently yield control above, to let our msg handler process
                    # received nodes (scheduling new requests), there may be cases when the
                    # pending nodes take a while to arrive thus causing the scheduler to run out
                    # of new requests for a while.
                    self.logger.debug("Scheduler queue is empty, sleeping a bit")
                    await self.sleep(0.5)
                    continue

                await self.request_nodes(requests)
                # await self.request_nodes([request.node_key for request in requests])

        self.logger.info(
            "Finished tree diff changed: %s unsure: %s elapsed: %s reqs: %s",
            self.scheduler.different_node_count,
            self.scheduler.unsure_node_count,
            self._timer.elapsed,
            self.packet_count,
        )

        depths = sorted(
            self.scheduler.diff_count_by_depth.items(),
            key=lambda item: item[0],
        )

        for depth, count in depths:
            self.logger.info(f"{depth:04}: {count}")

    async def _periodically_report_progress(self) -> None:
        while self.is_operational:
            requested_nodes = sum(
                len(node_keys) for _, node_keys in self.request_tracker.active_requests.values())
            msg = "processed=%d  " % self._total_processed_nodes
            msg += "tnps=%d  " % (self._total_processed_nodes / self._timer.elapsed)
            msg += "active_requests=%d  " % requested_nodes
            msg += "queued=%d  " % len(self.scheduler.queue)
            msg += "fetched=%d  " % len(self.scheduler.fetched_requests)
            msg += "different=%d  " % self.scheduler.different_node_count
            msg += "unsure=%d  " % self.scheduler.unsure_node_count
            msg += "pending=%d  " % len(self.scheduler.node_to_request)
            msg += "missing=%d  " % len(self.request_tracker.missing)
            msg += "timeouts=%d" % self._total_timeouts
            self.logger.info("State-Sync: %s", msg)
            await self.sleep(self._report_interval)


class TrieNodeRequestTracker:

    def __init__(self, reply_timeout: int, logger: ExtendedDebugLogger) -> None:
        self.reply_timeout = reply_timeout
        self.logger = logger
        self.active_requests: Dict[ETHPeer, Tuple[float, Tuple[Hash32, ...]]] = {}
        self.missing: Dict[float, List[Hash32]] = {}

    def get_timed_out(self) -> List[Hash32]:
        timed_out = eth_utils.toolz.valfilter(
            lambda v: time.time() - v[0] > self.reply_timeout, self.active_requests)
        for peer, (_, node_keys) in timed_out.items():
            self.logger.debug(
                "Timed out waiting for %d nodes from %s", len(node_keys), peer)
        self.active_requests = eth_utils.toolz.dissoc(self.active_requests, *timed_out.keys())
        return list(eth_utils.toolz.concat(node_keys for _, node_keys in timed_out.values()))

    def get_retriable_missing(self) -> List[Hash32]:
        retriable = eth_utils.toolz.keyfilter(
            lambda k: time.time() - k > self.reply_timeout, self.missing)
        self.missing = eth_utils.toolz.dissoc(self.missing, *retriable.keys())
        return list(eth_utils.toolz.concat(retriable.values()))

    def get_next_timeout(self) -> float:
        active_req_times = [req_time for (req_time, _) in self.active_requests.values()]
        oldest = min(itertools.chain([time.time()], self.missing.keys(), active_req_times))
        return oldest + self.reply_timeout


class StateSync(HexaryTrieSync):

    async def leaf_callback(self, data: bytes, parent: SyncRequest) -> None:
        # TODO: Need to figure out why geth uses 64 as the depth here, and then document it.
        depth = 64
        account = rlp.decode(data, sedes=Account)
        if account.storage_root != BLANK_ROOT_HASH:
            await self.schedule(account.storage_root, parent, depth, leaf_callback=None)
        if account.code_hash != EMPTY_SHA3:
            await self.schedule(account.code_hash, parent, depth, leaf_callback=None, is_raw=True)


class SingleAddressStateSync:
    "Does a state sync but only attempts to sync data for a single address"
    def __init__(self,
                 root_hash: Hash32,
                 db: AsyncBaseDB,
                 nodes_cache: BaseDB,
                 logger: ExtendedDebugLogger,
                 loop: asyncio.AbstractEventLoop) -> None:
        self.db = db
        self.nodes_cache = nodes_cache
        self.logger = logger
        self.loop = loop

        self.address = 'd0a6e6c54dbc68db5db3a091b171a77407ff7ccf'  # EOS
        self.address = '53a1e561f01ea6b6f39270fde1f15f5b71fcf300'
        self.address = remove_0x_prefix(encode_hex(keccak(decode_hex(self.address))))
        self.run_task(self.crawl(root_hash))

        self.committed_nodes = 0
        self.queue: List[Hash32] = list()
        self.requests: Dict[Hash32, asyncio.Future] = dict()

    def run_task(self, awaitable: Awaitable[Any]) -> None:
        @functools.wraps(awaitable)  # type: ignore
        async def _run_task_wrapper() -> None:
            try:
                await awaitable
            except OperationCancelled:
                pass
            except Exception as e:
                self.logger.warning("Task %s finished unexpectedly: %s", awaitable, e)
                self.logger.debug("Task failure traceback", exc_info=True)
        self.loop.create_task(_run_task_wrapper())

    @property
    def has_pending_requests(self) -> bool:
        # run forever
        return True

    def new_root_hash(self, root_hash: Hash32):
        self.run_task(self.crawl(root_hash))

    def next_batch(self, n: int = 1) -> List[SyncRequest]:
        # StateDownloader is asking for the next few nodes it should request
        if n >= len(self.queue):
            result = self.queue
            self.queue = list()
            return result

        result = self.queue[:n]
        self.queue = self.queue[n:]
        return result

    async def process(self, results: List[Tuple[Hash32, bytes]]) -> None:
        # StateDownloader received some nodes, handle them!
        for node_key, data in results:
            request = self.requests.pop(node_key, None)
            if request is None:
                self.logger.debug("%s was probably already fired", encode_hex(node_key))
            else:
                request.set_result(data)

    async def crawl(self, root_hash: Hash32) -> None:
        await asyncio.sleep(1)  # giving parity a second to build the state root
        ident = encode_hex(root_hash[:4])
        self.logger.debug("[%s] starting crawl", ident)
        node = await self._fetch_node(root_hash)
        node_type = get_node_type(node)

        trail = []
        depth = 0
        while True:
            if node_type == NODE_TYPE_BLANK:
                self.logger.error("[%s] received blank node", ident)
                return
            elif node_type == NODE_TYPE_LEAF:
                leaf = node[1]
                account = rlp.decode(leaf, sedes=Account)
                self.logger.debug(
                    "[%s] found account {%s, %s}. trail: %s",
                    ident, account.nonce, account.balance, trail
                )
                return
            elif node_type == NODE_TYPE_EXTENSION:
                self.logger.error("[%s] don't know how to handle extensions", ident)
                return
            elif node_type == NODE_TYPE_BRANCH:
                self.logger.error("[%s] got branch, at depth %s", ident, depth)
                index = int(self.address[depth], 16)
                to_fetch = node[index]
                if to_fetch == b'':
                    self.logger.error("[%s] found 0x, %s does not exist", ident, self.address)
                    return
                trail.append(encode_hex(to_fetch[:6]))
                node = await self._fetch_node(to_fetch)
                node_type = get_node_type(node)
                depth += 1
                continue
            else:
                self.logger.error('[%s] unexpected node type: %s', ident, node_type)
                return

    async def _fetch_node(self, node_key: Hash32):
        future = self.loop.create_future()
        self.queue.append(node_key)
        if self.requests.get(node_key, None):
            self.logger.warning("%s is already being fetched!", encode_hex(node_key))
            future = self.requests[node_key]
        else:
            self.requests[node_key] = future
        data = await future
        node = decode_node(data)
        return node


class DiffTraverse:
    def __init__(self, depth, left, right):
        self.depth = depth
        self.keys = [left, right]
        self.fetched = [False, False]
        self.data = [None, None]

    @property
    def ready(self):
        return self.fetched[0] and self.fetched[1]

    def fetch(self, key, data):
        try:
            index = self.keys.index(key)
        except ValueError:
            # broken invariant
            raise
        self.fetched[index] = True
        self.data[index] = data


class NormalTraverse:
    def __init__(self, depth, key: Hash32, unsure: bool = False):
        self.depth = depth
        self.key = key
        self.data = None
        self.unsure = unsure

    @property
    def ready(self):
        return self.data is not None

    def fetch(self, key, data):
        assert key == self.key
        self.data = data


class SearchTraverse:
    # An idea for correctly dealing with compare_extension_branch:
    # 2. You could start a SearchTraverse. The branch is traversed and all paths it
    # leads to spawn NormalTraverse's except for the one which leads to the previous
    # extension's target, that one continues to be a SearchTraverse. Once you've found
    # the target node you continue with a DiffTraverse.
    # - SearchTraverse is also careful not to skip past the target node. If it does
    # then that node no longer exists and we can continue with a NormalTraverse.
    pass


class TreeDiffStateSync:
    "Given two tree roots, fetches and counts the number of changed nodes"
    def __init__(self, left_root: Hash32, right_root: Hash32,
                 nodes_cache: BaseDB, logger: ExtendedDebugLogger) -> None:
        self.finished = False
        self.nodes_cache = nodes_cache
        self.logger = logger

        self.node_to_request: Dict[Hash32, Union[NormalTraverse, DiffTraverse]] = dict()
        self.fetched_requests = collections.deque()
        self.queue = list()

        self.different_node_count = 0
        self.unsure_node_count = 0
        self.diff_count_by_depth = collections.defaultdict(int)

        self.schedule_diff(0, left_root, right_root)

    @property
    def has_pending_requests(self) -> bool:
        if self.finished:
            return False
        return len(self.queue) or len(self.fetched_requests) or len(self.node_to_request)

    def next_batch(self, n: int = 1) -> List[SyncRequest]:
        # StateDownloader is asking for the next few nodes it should request
        if self.finished:
            return None

        while len(self.queue) < n and len(self.fetched_requests) and not self.finished:
            # process received nodes until we can fill a request
            req = self.fetched_requests.popleft()
            if isinstance(req, DiffTraverse):
                self.handle_diff_traverse(req)
            elif isinstance(req, NormalTraverse):
                self.handle_normal_traverse(req)
            else:
                assert False

        if len(self.queue):
            self.logger.info(
                "returning more nodes. diff: %s unsure: %s",
                self.different_node_count, self.unsure_node_count
            )

        result = self.queue[:n]
        self.queue = self.queue[n:]
        return result
    
    async def process(self, results: List[Tuple[Hash32, bytes]]) -> None:
        # StateDownloader received some nodes, handle them!
        for node_key, data in results:
            node = decode_node(data)
            request = self.node_to_request.pop(node_key)
            request.fetch(node_key, node)
            if request.ready:
                self.fetched_requests.append(request)

    def schedule_diff(self, depth, left: Hash32, right: Hash32) -> None:
        self.different_node_count += 1
        self.diff_count_by_depth[depth] += 1

        if BLANK_ROOT_HASH in (left, right):
            # this is typically an empty state root
            self.logger.error(
                'found BLANK_HASH in one branch and a node in the other at depth %s', depth
            )
            self.schedule_normal_traverse(
                depth, [item for item in (left, right) if item is not BLANK_ROOT_HASH]
            )
            return
        if BLANK_NODE in (left, right):
            # this is typically an empty child of a branch
            self.logger.debug(
                '(depth %s) found BLANK_NODE', depth
            )
            self.schedule_normal_traverse(
                depth, [item for item in (left, right) if item is not BLANK_NODE]
            )
            return

        request = DiffTraverse(depth, left, right)
        self.node_to_request[left] = request
        self.node_to_request[right] = request
        if b'' in (left, right):
            assert False
        self.queue.extend([left, right])

    def schedule_normal_traverse(self, depth, 
                                 children: List[Hash32],
                                 unsure: bool = False) -> None:
        for child in children:
            if child == BLANK_ROOT_HASH:
                continue
            if child == BLANK_NODE:
                continue
            if unsure:
                self.unsure_node_count += 1
            else:
                self.different_node_count += 1
                self.diff_count_by_depth[depth] += 1
            traverse = NormalTraverse(depth, child, unsure=unsure)
            self.node_to_request[child] = traverse
            self.queue.append(child)

    def schedule_normal_traverse_leaf(self, depth, leaf, unsure: bool = False) -> None:
        account = rlp.decode(leaf[1], sedes=Account)
        if account.code_hash != EMPTY_SHA3:
            # this is a node a state syncer would need to fetch
            if unsure:
                self.unsure_node_count += 1
            else:
                self.different_node_count += 1
                self.diff_count_by_depth[depth] += 1
        if account.storage_root != BLANK_ROOT_HASH:
            self.logger.debug(
                "(depth %s) traversing into a storage root", depth
            )
            self.schedule_normal_traverse(
                depth, [account.storage_root], unsure,
            )

    def handle_normal_traverse(self, traverse: NormalTraverse):
        node = traverse.data
        node_type = get_node_type(node)

        if node_type == NODE_TYPE_BRANCH:
            self.schedule_normal_traverse(
                traverse.depth + 1, node[:16], traverse.unsure
            )
        elif node_type == NODE_TYPE_LEAF:
            # there are two possibilities. Either we're a leaf in the state trie or we're
            # a leaf in the account's state trie. Add 64 to signal to ourselves that we've
            # entered an account's state trie
            if traverse.depth < 64:
                self.schedule_normal_traverse_leaf(
                    traverse.depth + 64, node, traverse.unsure
                )
        elif node_type == NODE_TYPE_EXTENSION:
            self.schedule_normal_traverse(
                traverse.depth + 1, [node[1]], traverse.unsure
            )
        else:
            self.logger.error("Unknown node type %s", node_type)
            self.logger.debug("Node: %s", node)
            self.finished = True

    def handle_diff_traverse(self, request: DiffTraverse):
        # once a request has come back we get to look at the two nodes!
        left_node, right_node = request.data
        left_type, right_type = map(get_node_type, request.data)

        # TODO: using the same method for both isn't correct, we care about the
        # number of new nodes and this erases information about which is new
        if left_type == NODE_TYPE_BRANCH and right_type == NODE_TYPE_BRANCH:
            self.compare_branches(request.depth, left_node, right_node)

        elif left_type == NODE_TYPE_LEAF and right_type == NODE_TYPE_LEAF:
            self.compare_leaves(request.depth, left_node, right_node)

        elif right_type == NODE_TYPE_EXTENSION and left_type == NODE_TYPE_EXTENSION:
            self.compare_extensions(request.depth, left_node, right_node)

        elif left_type == NODE_TYPE_BRANCH and right_type == NODE_TYPE_LEAF:
            self.compare_leaf_branch(request.depth, right_node, left_node)
        elif right_type == NODE_TYPE_BRANCH and left_type == NODE_TYPE_LEAF:
            self.compare_leaf_branch(request.depth, left_node, right_node)

        elif left_type == NODE_TYPE_EXTENSION and right_type == NODE_TYPE_BRANCH:
            self.compare_extension_branch(request.depth, left_node, right_node)
        elif left_type == NODE_TYPE_BRANCH and right_type == NODE_TYPE_EXTENSION:
            self.compare_extension_branch(request.depth, right_node, left_node)

        elif left_type == NODE_TYPE_EXTENSION and right_type == NODE_TYPE_LEAF:
            self.compare_extension_leaf(request.depth, left_node, right_node)
        elif left_type == NODE_TYPE_LEAF and right_type == NODE_TYPE_EXTENSION:
            self.compare_extension_leaf(request.depth, right_node, left_node)
        else:
            self.logger.debug(
                "(depth %s) Don't know how to handle node type (%s, %s)",
                request.depth, left_type, right_type
            )
            self.logger.debug("Left: %s", left_node)
            self.logger.debug("Right: %s", right_node)
            self.finished = True

    def format_branch(self, node):
        return [encode_hex(child[:6]) for child in node]

    def compare_leaf_branch(self, depth, leaf, branch):
        # where there was previously just one account there are now many
        # TODO: We care about the number of new nodes and this (over)counts how
        # many nodes belong to one tree but not the intersection of both.

        # these two nodes have already been counted as different by compare_branches but
        # we should also count the number of nodes in each
        if depth < 64:
            self.logger.debug("(depth %s) Reached leaf-branch, traversing into both", depth)
            self.schedule_normal_traverse(depth + 1, children=branch[:16], unsure=True)
            self.schedule_normal_traverse_leaf(depth + 64, leaf, unsure=True)
        else:
            # we've reached the bottom of the state tree!
            self.logger.debug("(depth %s) Reached state leaf-branch, traversing the branch", depth)
            self.schedule_normal_traverse(depth + 1, children=branch[:16])

    def compare_leaves(self, depth, left, right):
        if depth >= 64:
            # we've reached bottom of the state tree! No need to count any further
            return

        left_acct = rlp.decode(left[1], sedes=Account)
        right_acct = rlp.decode(right[1], sedes=Account)

        if left_acct.storage_root != right_acct.storage_root:
            self.logger.debug("(depth %s) leaf-leaf storage root changed", depth)
            self.schedule_diff(depth + 64, left_acct.storage_root, right_acct.storage_root)
        # I think the code-hash is immutable, so we needn't consider it here?

    def compare_branches(self, depth, left, right):
        for (lchild, rchild) in zip(left[:16], right[:16]):
            if lchild != rchild:
                self.schedule_diff(depth + 1, lchild, rchild)

    def compare_extensions(self, depth, left, right):
        self.logger.error(
            "(depth %s) two extensions", depth
        )
        if left[0] == right[0]:
            # the easy case, they both point to the same (changed) node
            self.schedule_diff(depth + 1, left[1], right[1])
            return

        # the rest of the cases are tricky to handle. a node has been added/removed
        # somewhere along the line. Just count the number of nodes in both
        self.schedule_normal_traverse(depth + 1, [left[1]], unsure=True)
        self.schedule_normal_traverse(depth + 1, [right[1]], unsure=True)

    def compare_extension_branch(self, depth, extension, branch):
        # the proper solution would involve remembering the extension's path and trying to
        # link it up with a node found inside branch. All other nodes count as changes.

        # For now count all nodes inside either as having been changed. Pass "unsure"
        # because this will usually overcount the number of changed nodes, this count will
        # be reported separately.
        self.logger.error(
            "(depth %s) extension-branch. following both", depth
        )
        self.schedule_normal_traverse(depth + 1, branch[:16], unsure=True)
        self.schedule_normal_traverse(depth + 1, [extension[1]], unsure=True)

    def compare_extension_leaf(self, depth, extension, leaf):
        # we used to have an extension and now we have a leaf:
        # - there were multiple nodes and all but one was deleted
        # we used to have a leaf and now we have an extension
        # - there was an account but now there are more

        if depth < 64:
            self.logger.error("(depth %s) extension-leaf. following both", depth)
            self.schedule_normal_traverse(depth + 1, children=[extension[1]], unsure=True)
            self.schedule_normal_traverse_leaf(depth + 64, leaf, unsure=True)
        else:
            self.logger.error("(depth %s) state extension-leaf. following the extension", depth)
            self.schedule_normal_traverse(depth + 1, children=[extension[1]])



def _test() -> None:
    import argparse
    import signal
    from pathlib import Path
    from eth.chains.ropsten import ROPSTEN_VM_CONFIGURATION
    from eth.chains.mainnet import MAINNET_VM_CONFIGURATION
    from eth.exceptions import CanonicalHeadNotFound
    from p2p import ecies
    from p2p.kademlia import Node
    from trinity.config import ChainConfig
    from trinity.constants import DEFAULT_PREFERRED_NODES, ROPSTEN_NETWORK_ID, MAINNET_NETWORK_ID
    from trinity.initialization import initialize_database
    from trinity.protocol.common.context import ChainContext
    from trinity.protocol.eth.servers import ETHRequestServer
    from trinity._utils.chains import load_nodekey
    from tests.core.integration_test_helpers import (
        FakeAsyncChainDB, FakeAsyncLevelDB, connect_to_peers_loop)
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('-db', type=str, required=True)
    parser.add_argument('-debug', action="store_true")
    parser.add_argument('-enode', type=str, required=False, help="The enode we should connect to")
    parser.add_argument('-nodekey', type=str)
    args = parser.parse_args()

    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG

    db = FakeAsyncLevelDB(args.db)
    chaindb = FakeAsyncChainDB(db)

    # doesn't work because this fake db isn't allow to write to the database?
    try:
        chaindb.get_canonical_head()
    except CanonicalHeadNotFound:
        chain_config = ChainConfig.from_preconfigured_network(MAINNET_NETWORK_ID)
        chain_config.initialize_chain(chaindb)

    network_id = MAINNET_NETWORK_ID
    if args.enode:
        nodes = tuple([Node.from_uri(args.enode)])
    else:
        nodes = DEFAULT_PREFERRED_NODES[network_id]

    if args.nodekey:
        privkey = load_nodekey(Path(args.nodekey))
    else:
        privkey = ecies.generate_privkey()

    context = ChainContext(
        headerdb=chaindb,
        network_id=network_id,
        vm_configuration=MAINNET_VM_CONFIGURATION,
    )
    peer_pool = ETHPeerPool(
        privkey=privkey,
        context=context,
    )

    # without this parity asks about the DAO, times out, and disconnects from us
    request_server = ETHRequestServer(chaindb, peer_pool)

    asyncio.ensure_future(peer_pool.run())
    asyncio.ensure_future(request_server.run())
    peer_pool.run_task(connect_to_peers_loop(peer_pool, nodes))

    # head = chaindb.get_canonical_head()
    downloader = StateDownloader(chaindb, db, peer_pool)
    downloader.logger.setLevel(log_level)
    loop = asyncio.get_event_loop()

    sigint_received = asyncio.Event()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, sigint_received.set)

    async def exit_on_sigint() -> None:
        await sigint_received.wait()
        await downloader.cancel()
        await request_server.cancel()
        await peer_pool.cancel()
        loop.stop()

    async def run() -> None:
        await downloader.run()
        downloader.logger.info("run() finished, exiting")
        sigint_received.set()

    # loop.set_debug(True)
    asyncio.ensure_future(exit_on_sigint())
    asyncio.ensure_future(run())
    loop.run_forever()
    loop.close()


if __name__ == "__main__":
    # Use the snippet below to get profile stats and print the top 50 functions by cumulative time
    # used.
    # import cProfile, pstats  # noqa
    # cProfile.run('_test()', 'stats')
    # pstats.Stats('stats').strip_dirs().sort_stats('cumulative').print_stats(50)
    _test()
