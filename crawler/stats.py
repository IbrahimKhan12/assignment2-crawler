from urllib.parse import urldefrag, urlparse

class Stats:
    def __init__(self):
        self.unique_urls = set()
        self.longest_page = ''
        self.longest_page_words = 0
        self.word_counts = {}
        self.subdomains = {}
        
    def add_url(self, url):
        url, _ = urldefrag(url)
        if url not in self.unique_urls:
            self.unique_urls.add(url)
            parsed = urlparse(url)
            if parsed.netloc.endswith(".uci.edu"):
                subdomain = parsed.netloc.lower()
                self.subdomains[subdomain] = self.subdomains.get(subdomain, 0) + 1
    
    def update_longest_page(self, url, word_count):
        if word_count > self.longest_page_words:
            self.longest_page_words = word_count
            self.longest_page = url
                
    def add_words(self, words):
        for word in words:
            if word in self.word_counts:
                self.word_counts[word] += 1
            else:
                self.word_counts[word] = 1
    
    def get_unique_pages(self):
        return len(self.unique_urls)
            
    def get_longest_page(self):
        return (self.longest_page, self.longest_page_words)
    
    def get_top_50(self):
        sorted_words = sorted(self.word_counts.items(), key=lambda x: x[1], reverse=True)
        return sorted_words[:50]
    
    def get_subdomains(self):
        return sorted(self.subdomains.items())
