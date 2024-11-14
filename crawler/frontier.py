import os
import shelve
import threading
import time
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid
from urllib.parse import urldefrag, urlparse

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.domain_queues = {}
        self.last_access_time = {}
        self.save_lock = threading.Lock()  # Added lock for self.save
        
        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.add_url(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        with self.condition:
            while True:
                earliest_time = None
                current_time = time.time()
                for domain in list(self.domain_queues.keys()):
                    if self.domain_queues[domain]:
                        last_time = self.last_access_time.get(domain, 0)
                        wait_time = self.config.time_delay - (current_time - last_time)
                        if wait_time <= 0:
                            url = self.domain_queues[domain].pop(0)
                            self.last_access_time[domain] = time.time()
                            return url
                        else:
                            next_available_time = last_time + self.config.time_delay
                            if earliest_time is None or next_available_time < earliest_time:
                                earliest_time = next_available_time
                if earliest_time is not None:
                    sleep_time = earliest_time - time.time()
                    if sleep_time > 0:
                        self.condition.wait(timeout=sleep_time)
                    else:
                        continue
                else:
                    if all(not q for q in self.domain_queues.values()):
                        return None
                    else:
                        self.condition.wait()
    
    def add_url(self, url):
        url, _ = urldefrag(url)
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.save_lock:
            if urlhash in self.save:
                return
        parsed = urlparse(url)
        domain = parsed.netloc
        with self.condition:
            if domain not in self.domain_queues:
                self.domain_queues[domain] = []
            self.domain_queues[domain].append(url)
            with self.save_lock:
                self.save[urlhash] = (url, False)
                self.save.sync()
            self.condition.notify_all()
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.save_lock:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            else:
                self.save[urlhash] = (url, True)
                self.save.sync()
