from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from crawler.simhash import SimHash
class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker, simhash_factory=SimHash, pickle_file_prefix="stats"):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.simhash = simhash_factory(config, restart) if config.simhash_file is not None else None
        self.pickle_file_prefix = pickle_file_prefix
        self.workers = list()
        self.worker_factory = worker_factory
        self.restart = restart

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.simhash, self.restart)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        try:
            self.start_async()
        finally:
            self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
