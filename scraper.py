import re

from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from hashlib import md5
from simhash import Simhash
from collections import defaultdict
from urllib.robotparser import RobotFileParser
import urllib.error
from utils.download import download
from utils.config import Config

# Global trap detection state (persists across scraper calls)
class TrapDetector:
    """
    This class detects potential traps in webpages based on URL patterns and content similarity.
    """
    def __init__(self):
        """
        Initializes the TrapDetector with data structures to track URL patterns and content hashes.
        """
        self.url_pattern_count = defaultdict(int)
        self.content_hashes = set() # Stores Simhash values
        self.SUSPICIOUS_PATHS = defaultdict(int)
        self.visited_urls = set()  # Stores already processed URLs
    
    def is_trap_url(self, url):
        """
        Checks if the given URL exhibits patterns that might indicate a trap,
        such as excessive path depth, repeated segments, or event pages with dates.
        """
        parsed = urlparse(url)
        path = parsed.path

        # Check for excessive path depth
        if len(path.split('/')) > 10:
            return True

        # Check for numeric patterns in URL (e.g., very long numbers)
        if re.search(r'/\d{5,}', path):
            return True

        # Check for repeated path segments
        path_segments = [s for s in path.split('/') if s]
        if len(path_segments) != len(set(path_segments)):
            return True

        # Check: if the path starts with /event/ or /events/ and has additional content
        # that contains a date pattern (e.g., 2024-02-06), then treat it as a trap.
        if self.is_event_date_trap(url):
            return True

        return False

    def is_event_date_trap(self, url):
        """
        Returns True if the URL's path starts with /event/ or /events/ and contains a date pattern,
        indicating that it is a duplicate or near-duplicate event page.
        """
        parsed = urlparse(url)
        path = parsed.path

        # Only trigger if the path begins with /event/ or /events/
        if path.startswith("/event/") or path.startswith("/events/"):
            # Allow the base path "/event/" or "/events/" (with or without a trailing slash)
            if path in ["/event", "/event/", "/events", "/events/"]:
                return False

            # Use a regex to search for common date patterns in the additional path
            date_pattern = r"\d{4}[-/]\d{2}([-/]\d{2})?"  # Matches YYYY-MM or YYYY-MM-DD
            if re.search(date_pattern, path):
                return True

        return False

    def is_duplicate_content(self, content):
        """
        Detects near-duplicate content using Simhash.
        """
        # Extract text content
        soup = BeautifulSoup(content, "html.parser")
        text_content = " ".join(soup.stripped_strings)

        # Compute Simhash for this content
        simhash_value = Simhash(text_content).value
        threshold = 5  # Allow small variations (Hamming distance)

        # Check if the page is similar to any seen page
        for seen_hash in self.content_hashes:
            if bin(seen_hash ^ simhash_value).count("1") <= threshold:
                return True  # Similar content detected
        
        # Store the new content hash
        self.content_hashes.add(simhash_value)
        return False

    def is_duplicate_url(self, url):
        """Checks if a URL has already been processed."""
        if url in self.visited_urls:
            return True
        self.visited_urls.add(url)
        return False

# Initialize the global trap detector
TRAP_DETECTOR = TrapDetector()

def scraper(url, resp, config):
    """
    This is the main scraper function that extracts links from the given webpage.

    Args:
        url (str): The original URL requested.
        resp (Response): The response object containing the webpage content.

    Returns:
        list: A list of valid URLs extracted from the webpage.
    """
    # Handle redirects by using final URL
    final_url = resp.url if resp.url else url
    
    # Check for dead URLs (200 status but very little content)
    if resp.status == 200 and resp.raw_response and hasattr(resp.raw_response, 'content') and len(resp.raw_response.content) < 512:
        return []
    
    # Check for large files (>2MB) and skip them
    if resp.raw_response and hasattr(resp.raw_response, 'content') and resp.raw_response.content:
        content_length = len(resp.raw_response.content)  # Get actual content length
        if content_length > 2 * 1024 * 1024:  # 2MB threshold
            return []

    # Check content type, only process text-based HTML pages
    content_type = ''
    if resp.raw_response and hasattr(resp.raw_response, 'headers') and resp.raw_response.headers:
        content_type = resp.raw_response.headers.get('Content-Type', '')

    if not re.match(r'^text/html(;\s*charset=.*)?$', content_type):
        return []
    
    # Trap detection checks
    if TRAP_DETECTOR.is_duplicate_url(final_url):
        return []

    if TRAP_DETECTOR.is_trap_url(final_url):
        return []
    
    if TRAP_DETECTOR.is_duplicate_content(resp.raw_response.content):
        return []
    
    # Extract and process valid links
    links = extract_next_links(final_url, resp)
    valid_links = [link for link in links if is_valid(link, config)]

    return valid_links

def extract_next_links(url, resp):
    """
    Extracts all hyperlinks from the page content.
    """
    links = []
    
    if resp.status!= 200:
        return links
    
    try:
        if resp.raw_response and hasattr(resp.raw_response, 'content') and resp.raw_response.content:
            soup = BeautifulSoup(resp.raw_response.content, 'lxml')
        else:
            return []  # Skip processing if no valid content
        for link in soup.find_all('a', href=True):
            # Convert to absolute URL
            absolute_url = urljoin(url, link['href'])
            # Remove URL fragment
            defragged_url = urldefrag(absolute_url).url
            links.append(defragged_url)
    except Exception as e:
        print(f"BeautifulSoup parsing error at {url}: {str(e)}")
        return []
        
    return links

# Cache for robots.txt rules
robots_txt_cache = {}

def is_valid(url, config):
    """
    Validation checks for URLs before crawling.
    Ensures adherence to allowed domains, robots.txt, and avoids unnecessary file types.
    """
    try:
        parsed = urlparse(url)
        
        # Check scheme
        if parsed.scheme not in set(["http", "https"]):
            return False
            
        # Check allowed domains
        allowed_domains = {
            'ics.uci.edu',
            'cs.uci.edu',
            'informatics.uci.edu',
            'stat.uci.edu'
        }
        if not any(parsed.netloc.endswith(domain) for domain in allowed_domains):
            return False
            
        # Check file extensions
        if re.search(
            r'\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4'
            r'|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf'
            r'|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names'
            r'|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso'
            r'|epub|dll|cnf|tgz|sha1|thmx|mso|arff|rtf|jar|csv'
            r'|rm|smil|wmv|swf|wma|zip|rar|gz)$', parsed.path.lower()):
            return False
            
        # Check for ASCII-only URLs
        try:
            if isinstance(url, bytes):  
                url.decode('ascii')  
            else:
                url.encode('ascii')  
        except UnicodeEncodeError:
            return False
            
        # Check query parameters for traps
        if parsed.query:
            if re.search(r'page=\d{4,}', parsed.query.lower()):  # Blocks page=1000+
                return False
            if 'sessionid' in parsed.query.lower():
                return False
        
        domain = parsed.netloc
        try:
            # Check if robots.txt is cached for this domain
            if domain not in robots_txt_cache:
                robots_url = f"{parsed.scheme}://{domain}/robots.txt"
                try:
                    response = download(robots_url, config, timeout=10)
                except Exception as e:
                    print(f"Robots.txt download failed: {str(e)}")
                    robots_txt_cache[domain] = None
                
                rp = RobotFileParser()
                if response and response.raw_response:
                    robots_content = response.raw_response.content.decode("utf-8")
                    rp.parse(robots_content.splitlines())  # Load robots.txt rules
                    robots_txt_cache[domain] = rp  # Cache for future use
                else:
                    robots_txt_cache[domain] = None  # Mark as unavailable


            # Use cached robots.txt
            rp = robots_txt_cache.get(domain)
            if not rp:
                return True  # If robots.txt couldn't be retrieved, allow crawling

            # Check if the path is disallowed
            if rp and not rp.can_fetch("*", url):
                return False
            
        except Exception as e:
            print(f"Error processing robots.txt for {url}: {e}")

        return True

    except Exception as e:
        print(f"Validation error for {url}: {str(e)}")
        return False