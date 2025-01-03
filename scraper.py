import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200:
        return []
    if resp.raw_response is None:
        return []
    if resp.raw_response.content is None:
        return []
    if len(resp.raw_response.content) == 0:
        return []
    
    # make it a set to avoid duplicates
    links = set()
    # beautiful soup is HTML parser
    souped = BeautifulSoup(resp.raw_response.content, 'lxml')

    # save all a tags because they contain URLs
    for atag in souped.find_all('a'):
        href = atag.get('href')
        if href:
            href, _ = urldefrag(href)
            new_url = urljoin(url, href)
            links.add(new_url)
    # return the set as a list because that's how implementation is meant to be
    return list(links)

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        
        # parse the url. example:
        # urlparse("http://docs.python.org:80/3/library/urllib.parse.html?"
        #     "highlight=params#url-parsing")
        # ParseResult(scheme='http', netloc='docs.python.org:80',
        #     path='/3/library/urllib.parse.html', params='',
        #     query='highlight=params', fragment='url-parsing')
        parsed = urlparse(url)
        
        # get rid of non http and https urls
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # get rid of anything outside our crawling conditions
        domain = parsed.netloc.lower()
        path = parsed.path.lower()
        if not (
            domain.endswith(".ics.uci.edu") or domain == "ics.uci.edu" or
            domain.endswith(".cs.uci.edu") or domain == "cs.uci.edu" or
            domain.endswith(".informatics.uci.edu") or domain == "informatics.uci.edu" or
            domain.endswith(".stat.uci.edu") or domain == "stat.uci.edu" or
            (domain == "today.uci.edu" and path.startswith("/department/information_computer_sciences"))
        ):
            return False
        
        # get rid of traps i fell into
        if any(param in parsed.query for param in ["do=login", "do=revisions", "do=edit", "do=media", "image=", "ical=1", "outlook-ical=1", "tribe-bar-date", "redirect", "share=", "filter", "action=download", "action=login", "idx=", "version=", "precision=", "rev="]):
            return False
        
        if any(param in parsed.path for param in ["/-/commit", "/-/blob", "/-/blame", "/-/tree", "/-/branches", "/-/forks", "/-/merge_requests", "~eppstein/pix/", "/zip-attachment/", "/attachment/", "/-/tags/"]):
            return False

        # get rid of useless files
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico|grm|psp|git"
            + r"|png|tiff?|mid|mp2|mp3|mp4|mpg|scm|bib|h"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1|txt|py|ff|cpp|md|cp"
            + r"|thmx|mso|arff|rtf|jar|csv|ppsx|java|patch"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|apk|war|img|sql)$", parsed.path.lower()):
            return False

        # get rid of too long URLs, prob traps
        if len(url) > 1000:
            return False
        
        # if a url repeats a segment too much, fix it
        path_segments = parsed.path.split('/')
        segment_counts = {}
        for segment in path_segments:
            if segment in segment_counts:
                segment_counts[segment] += 1
                if segment_counts[segment] >= 3:
                    return False
            else:
                segment_counts[segment] = 1

        return True

    except TypeError:
        print ("TypeError for ", parsed)
        raise