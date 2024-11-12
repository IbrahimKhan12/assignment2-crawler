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
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.add_url(url)  # Use add_url to handle the threading and data structures
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
                        continue  # Time has already passed, retry immediately
                else:
                    # No URLs are available; wait until notified
                    if all(not q for q in self.domain_queues.values()):
                        # All queues are empty; no more URLs to process
                        return None
                    else:
                        self.condition.wait()
    
    def add_url(self, url):
        url, _ = urldefrag(url)
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            parsed = urlparse(url)
            domain = parsed.netloc
            with self.condition:
                if domain not in self.domain_queues:
                    self.domain_queues[domain] = []
                self.domain_queues[domain].append(url)
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.condition.notify_all()
        
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")
        else:
            self.save[urlhash] = (url, True)
            self.save.sync()
