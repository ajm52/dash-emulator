import asyncio
import time
import queue
from typing import Optional

from dash_emulator import logger, events, config, events

# noinspection DuplicatedCode
log = logger.getLogger(__name__)


class SpeedMonitor(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
            cls._instance.inited = False
        return cls._instance

    def __init__(self):
        if not self.inited:
            self.inited = True
            self.cfg = None

            self.avg_bandwidth = 0
            self.last_speed = -1

            self.downloaded = 0
            self.downloaded_before = 0
            self._calculate_speed_task = None  # type: Optional[asyncio.Task]

    async def calculate_speed(self):
        while True:
            downloaded = self.downloaded
            await asyncio.sleep(0.3)
            await self.feed(self.downloaded - downloaded, 0.3)
            self.print()

    def init(self, cfg):
        self.cfg = cfg  # type: config.Config
        event_bridge = events.EventBridge()

        async def calculate_speed():
            try:
                self._calculate_speed_task = asyncio.create_task(
                    self.calculate_speed())
            except AttributeError:
                loop = asyncio.get_event_loop()
                self._calculate_speed_task = loop.create_task(
                    self.calculate_speed())

        event_bridge.add_listener(
            events.Events.MPDParseComplete, calculate_speed)

        async def download_complete(*args, **kwargs):
            self.downloaded_before = self.downloaded

        event_bridge.add_listener(
            events.Events.SegmentDownloadComplete, download_complete)

    async def stop(self):
        self._calculate_speed_task.cancel()

    async def feed(self, data, time):
        if self.last_speed < 0:
            self.last_speed = data / time
            self.avg_bandwidth = self.last_speed
        else:
            self.last_speed = data / time
            self.avg_bandwidth = self.cfg.smoothing_factor * self.last_speed + (
                1 - self.cfg.smoothing_factor) * self.avg_bandwidth

    def print(self):
        log.info("Avg bandwidth: %d bps" % (self.avg_bandwidth * 8))

    def get_speed(self):
        return self.avg_bandwidth


class BufferMonitor(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
            cls._instance.inited = False
        return cls._instance

    def __init__(self):
        if not self.inited:
            self.inited = True

            self.cfg = None
            self.buffer_condition = asyncio.Condition()
            self.buffer_full_condition = asyncio.Condition() 
            self._buffer = 0

    def init(self, cfg):
        self.cfg = cfg
        self.segment_queue_size = int(self.cfg.segment_duration * self.cfg.buffer_capacity)
        self.segment_queue = queue.Queue(maxsize=self.segment_queue_size) # our segment playback buffer

        async def feed_segment(duration, *args, **kwargs):
            await self.buffer_condition.acquire()
            self._buffer += duration
            self.segment_queue.put(duration)
            self.buffer_condition.release()
            await events.EventBridge().trigger(events.Events.BufferUpdated, buffer=self._buffer)

        events.EventBridge().add_listener(
            events.Events.SegmentDownloadComplete, feed_segment)

    @property
    def buffer(self):
        return self._buffer


class DownloadProgressMonitor(object):
    def __init__(self, cfg, session):
        self.segment_condition = asyncio.Condition()
        self.segment_length = 0
        self.task = None  # type: asyncio.Task
        self.config = cfg
        self.downloaded = 0
        self.session = session

    async def wait_then_check(self,timeout):
        """
        used purely to remove code duplication in the method below.
        """
        await asyncio.sleep(self.config.timeout_max_ratio - timeout)
        # re-check status of downloaded frames
        downloaded = self.downloaded
        downloaded_ratio = downloaded / length
        if downloaded_ratio < self.config.min_frame_chunk_ratio:
            # f_i < f_i^{min} (drop and replace single tile-segment @ lowest quality)
            if self.session.representation_indices[self.session.adaptation_set.id] != 0:
                self.task.cancel()
                await events.EventBridge().trigger(events.Events.RedoTileAtLowest,
                                                           adaptation_set=self.session.adaptation_set)
                return
            else:
                # f_i >= f_i^{min} (accept partial segment)
                self.task.cancel()

    async def start(self):
        """
        In MPEG-DASH, the player cannot download the segment completely, because of the bandwidth fluctuation
        :return: a coroutine object
        """

        if self.session.duration == 0:
            # for 'init-stream.m4s' files
            return

        print('BETA Algorithm: Start')

        bandwidth = -1
        if self.session.segment_index <= 0:
            bandwidth = self.config.max_initial_bitrate
        else:
            bandwidth = SpeedMonitor().get_speed()

        # ensuring segment length is set elsewhere before we access it here.
        await self.segment_condition.acquire()
        while self.segment_length == 0:
            try:
                await self.segment_condition.wait()
            except RuntimeError as e:
                await asyncio.sleep(0.1)
        length = self.segment_length
        self.segment_condition.release()

        timeout = length * 8 / bandwidth
        # if the segment is downloaded fully before timeout finishes, this task will be cancelled via DownloadManager.
        await asyncio.sleep(timeout)
        print('BETA: Initial timeout reached')

        buffer_level = BufferMonitor().buffer
        downloaded = self.downloaded
        start_time = time.time()
        downloaded_ratio = downloaded / length

        if downloaded_ratio < self.config.min_frame_chunk_ratio:
            if buffer_level < self.config.min_buffer_length:
                # l < l^{min} (drop and replace single tile-segment @ lowest quality)
                if self.session.representation_indices[self.session.adaptation_set.id] != 0:
                    self.task.cancel()
                    await events.EventBridge().trigger(events.Events.RedoTileAtLowest,
                                                       session=self.session)
                    return
            else:
                # otherwise, buffer level is healthy; we can wait a bit longer (t_max - timeout)
                await wait_then_check(self.config.timeout_max_ratio - timeout)

        else: # f_i^{min} <= f_i 
            if downloaded_ratio < self.config.vq_threshold:
                if buffer_level < self.config.min_buffer_length:
                    # l < l^{min} (drop and replace single tile-segment @ lowest quality)
                    if self.session.representation_indices[self.session.adaptation_set.id] != 0:
                        self.task.cancel()
                        await events.EventBridge().trigger(events.Events.RedoTileAtLowest,
                                                           session=self.session)
                        return
                else:
                    # otherwise, buffer level is healthy; we can wait a bit longer (t_max - timeout)
                    await wait_then_check(self.config.timeout_max_ratio - timeout)
            else:
                # f_i >= f_i^{VQ} (we accept a partial segment)
                self.task.cancel()
