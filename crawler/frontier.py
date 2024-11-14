import heapq
import os
import shelve
import threading
import time
from scraper import is_valid
from utils import get_logger, get_urlhash, normalize
from urllib.parse import urldefrag, urlparse

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.domain_queues = {}
        self.last_access_time = {}
        self.available_domains = []
        self.save_lock = threading.Lock()
        self.domains_in_heap = set()
        self.busy_domains = set()

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
                current_time = time.time()
                if not self.available_domains and not any(self.domain_queues.values()):
                    # Frontier is empty
                    return None

                while self.available_domains:
                    next_available_time, domain = heapq.heappop(self.available_domains)
                    if domain not in self.domain_queues or not self.domain_queues[domain]:
                        self.domains_in_heap.discard(domain)
                        continue  # No URLs left for this domain
                    if domain in self.busy_domains:
                        heapq.heappush(self.available_domains, (next_available_time, domain))
                        continue  # Domain is busy
                    if current_time >= next_available_time:
                        url = self.domain_queues[domain].pop(0)
                        self.busy_domains.add(domain)
                        # If there are more URLs in the domain, keep domain in heap
                        if self.domain_queues[domain]:
                            heapq.heappush(self.available_domains, (next_available_time, domain))
                            self.domains_in_heap.add(domain)
                        else:
                            self.domains_in_heap.discard(domain)
                        return url
                    else:
                        # Not yet time to crawl this domain
                        heapq.heappush(self.available_domains, (next_available_time, domain))
                        sleep_time = next_available_time - current_time
                        self.condition.wait(timeout=sleep_time)
                        break

                if not self.available_domains:
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
            if domain not in self.domains_in_heap:
                next_available = self.last_access_time.get(domain, 0)
                heapq.heappush(self.available_domains, (next_available, domain))
                self.domains_in_heap.add(domain)
            self.condition.notify_all()

    def mark_domain_done(self, domain):
        with self.lock:
            current_time = time.time()
            self.last_access_time[domain] = current_time
            self.busy_domains.discard(domain)
            if self.domain_queues.get(domain) and domain not in self.domains_in_heap:
                next_available_time = current_time + self.config.time_delay
                heapq.heappush(self.available_domains, (next_available_time, domain))
                self.domains_in_heap.add(domain)
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