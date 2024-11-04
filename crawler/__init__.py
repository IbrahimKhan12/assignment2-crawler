from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from crawler.stats import Stats

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.stats = Stats()
        self.workers = list()
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.stats)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
        self.output_stats()

    def output_stats(self):
        longest_page, longest_page_words = self.stats.get_longest_page()
        top_50 = self.stats.get_top_50()
        subdomains = self.stats.get_subdomains()
        
        with open('stats.txt', 'w') as f:
            f.write(f"Number of unique pages: {self.stats.get_unique_pages()}\n\n"
                f"Longest page: {longest_page} ({longest_page_words} words)\n\n"
                "50 most common words:\n")
            for word, count in top_50:
                f.write(f"  {word}: {count}\n")
            f.write("\nSubdomains under uci.edu:\n")
            for subdomain, count in subdomains:
                f.write(f"  {subdomain}, {count}\n")