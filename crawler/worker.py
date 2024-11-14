import hashlib
import re
import scraper
from bs4 import BeautifulSoup
from crawler.stats import Stats
from nltk.corpus import stopwords
from threading import Thread
from urllib.parse import urlparse
from utils import get_logger
from utils.download import download

stop_words = set(stopwords.words('english'))

class Worker(Thread):
    def __init__(self, worker_id, config, frontier, stats):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.stats = stats
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            self.frontier.mark_url_complete(tbd_url)
            if resp.status == 200 and resp.raw_response and resp.raw_response.content:
                words = self.get_words(resp.raw_response.content)
                if len(words) < 50:
                    self.logger.info(f"Page {tbd_url} ignored due to low word count ({len(words)}).")
                    parsed = urlparse(tbd_url)
                    domain = parsed.netloc
                    self.frontier.mark_domain_done(domain)
                    continue
                simhash = self.compute_simhash(words)
                if self.stats.similar(simhash):
                    self.logger.info(f"Page {tbd_url} is similar to an already seen page, skipping.")
                    parsed = urlparse(tbd_url)
                    domain = parsed.netloc
                    self.frontier.mark_domain_done(domain)
                    continue
                self.stats.add_url(tbd_url)
                self.stats.add_simhash(simhash)
                self.stats.add_words(words)
                self.stats.update_longest_page(tbd_url, len(words))
                scraped_urls = scraper.scraper(tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                parsed = urlparse(tbd_url)
                domain = parsed.netloc
                self.frontier.mark_domain_done(domain)
            else:
                self.logger.info(f"Skipping URL {tbd_url} due to status {resp.status}.")
                parsed = urlparse(tbd_url)
                domain = parsed.netloc
                self.frontier.mark_domain_done(domain)
                continue

    def get_words(self, content):
        soup = BeautifulSoup(content, 'lxml')
        text = soup.get_text()
        words = re.findall(r'\b[a-zA-Z]{2,}\b', text.lower())
        words = [word for word in words if word not in stop_words]
        return words
        
    def compute_simhash(self, words):
        hash_bits = 64
        v = [0] * hash_bits
        for word in words:
            hash_value = int(hashlib.md5(word.encode('utf-8')).hexdigest(), 16)
            for i in range(hash_bits):
                bitmask = 1 << i
                if hash_value & bitmask:
                    v[i] += 1
                else:
                    v[i] -= 1
        fingerprint = 0
        for i in range(hash_bits):
            if v[i] >= 0:
                fingerprint |= 1 << i
        return fingerprint