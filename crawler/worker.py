import hashlib
import scraper
from threading import Thread
from inspect import getsource
from utils.download import download
from utils import get_logger
from crawler.stats import Stats
from bs4 import BeautifulSoup
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

stop_words = set(stopwords.words('english'))

class Worker(Thread):
    def __init__(self, worker_id, config, frontier, stats):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.stats = stats
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
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
                # Parse the content to extract words
                words = self.get_words(resp.raw_response.content)
                simhash = self.compute_simhash(words)
                # Add the URL to statistics before checking similarity
                self.stats.add_url(tbd_url)
                if self.stats.similar(simhash):
                    self.logger.info(f"Page {tbd_url} is similar to an already seen page, skipping.")
                    continue  # Skip processing this page
                self.stats.add_simhash(simhash)
                self.stats.add_words(words)
                self.stats.update_longest_page(tbd_url, len(words))
                # Only scrape unique pages
                scraped_urls = scraper.scraper(tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            # Removed time.sleep(self.config.time_delay)
    
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
