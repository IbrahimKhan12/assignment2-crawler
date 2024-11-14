import bisect
import threading
from collections import defaultdict, Counter
from urllib.parse import urldefrag, urlparse

class Stats:
    def __init__(self):
        self.unique_urls = set()
        self.longest_page = ''
        self.longest_page_words = 0
        self.word_counts = Counter()
        self.subdomains = defaultdict(int)
        self.simhashes = []
        self.url_lock = threading.Lock()
        self.page_lock = threading.Lock()
        self.word_lock = threading.Lock()
        self.simhash_lock = threading.Lock()
        self.subdomain_lock = threading.Lock()

    def similar(self, simhash, threshold=3):
        with self.simhash_lock:
            index = bisect.bisect_left(self.simhashes, simhash)
            for i in range(max(0, index - 5), min(len(self.simhashes), index + 5)):
                existing_simhash = self.simhashes[i]
                if self.hamming_distance(simhash, existing_simhash) <= threshold:
                    return True
            return False

    def hamming_distance(self, hash1, hash2):
        x = hash1 ^ hash2
        dist = 0
        while x:
            dist += 1
            x &= x - 1
        return dist

    def add_simhash(self, simhash):
        with self.simhash_lock:
            bisect.insort(self.simhashes, simhash)
        
    def add_url(self, url):
        url, _ = urldefrag(url)
        parsed = urlparse(url)
        with self.url_lock:
            if url not in self.unique_urls:
                self.unique_urls.add(url)
                if parsed.netloc.endswith(".uci.edu"):
                    parts = parsed.netloc.lower().split('.')
                    if len(parts) >= 3:
                        subdomain = '.'.join(parts[-3:])
                    else:
                        subdomain = parsed.netloc.lower()
                    with self.subdomain_lock:
                        self.subdomains[subdomain] += 1

    def update_longest_page(self, url, word_count):
        with self.page_lock:
            if word_count > self.longest_page_words:
                self.longest_page_words = word_count
                self.longest_page = url
                
    def add_words(self, words):
        with self.word_lock:
            self.word_counts.update(words)
    
    def get_unique_pages(self):
        with self.url_lock:
            return len(self.unique_urls)
            
    def get_longest_page(self):
        with self.page_lock:
            return (self.longest_page, self.longest_page_words)
    
    def get_top_50(self):
        with self.word_lock:
            return self.word_counts.most_common(50)
    
    def get_subdomains(self):
        with self.subdomain_lock:
            return sorted(self.subdomains.items())