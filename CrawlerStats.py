import re
from urllib.parse import urlparse, urlunparse
from bs4 import BeautifulSoup
from collections import Counter

class CrawlerStats:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CrawlerStats, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance

    def __init__(self):
        # Prevent __init__ from being called more than once by the Singleton
        pass

    def initialize(self):
        """Initialize the statistics."""
        self.unique_pages = set()  # Set of unique pages (URLs without fragments)
        self.longest_page = None  # The longest page (in terms of words)
        self.word_count = Counter()  # Count of words
        self.subdomains = {}  # Subdomain statistics

        self.stop_words = set([
            "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", 
            "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", 
            "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", 
            "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", 
            "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", 
            "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", 
            "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", 
            "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", 
            "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", 
            "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", 
            "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", 
            "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", 
            "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", 
            "you've", "your", "yours", "yourself", "yourselves"
        ])

    def normalize_url(self, url):
        """Remove fragment part from URL (after #) to ensure uniqueness."""
        parsed_url = urlparse(url)
        return urlunparse(parsed_url._replace(fragment=''))

    def count_words(self, content):
        """Count words in the page content, ignoring HTML tags."""
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')
        
        # Extract text and split it into words
        text = soup.get_text()
        
        # Split the text into words and count them
        words = text.split()
        
        return len(words)

    def update_page(self, url, content):
        """Update the statistics with a new page."""
        url = self.normalize_url(url)
        self.unique_pages.add(url)
        self.update_longest_page(url, content)
        self.tokenize_and_count_words(content)
        self.update_subdomain(url)

    def update_longest_page(self, url, content):
        """Check if the current page is the longest."""
        word_count = self.count_words(content)
        if not self.longest_page or word_count > self.longest_page[1]:
            self.longest_page = (url, word_count)

    def tokenize_and_count_words(self, content):
        """Tokenize the page content and count words (excluding stop words)."""
        words = re.findall(r'\b\w+\b', content.lower())  # Extract words, case insensitive
        filtered_words = [word for word in words if word not in self.stop_words]
        self.word_count.update(filtered_words)

    def update_subdomain(self, url):
        """Update subdomain statistics."""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        # Check if it's within the ics.uci.edu domain
        if domain.endswith("ics.uci.edu"):
            # Extract the subdomain part
            subdomain = domain.split('.')[0]  # Get the part before 'ics.uci.edu'

            # Update subdomain count
            if subdomain not in self.subdomains:
                self.subdomains[subdomain] = set()  # Use set to track unique pages
            self.subdomains[subdomain].add(url)  # Add URL to the set of the subdomain

    def get_unique_page_count(self):
        """Return the number of unique pages found."""
        return len(self.unique_pages)

    def get_longest_page(self):
        """Return the longest page URL and word count."""
        return self.longest_page

    def get_most_common_words(self, n=50):
        """Return the n most common words across all pages crawled."""
        return self.word_count.most_common(n)

    def get_subdomain_stats(self):
        """Return a list of subdomains ordered alphabetically with the number of unique pages detected in each subdomain."""
        # Prepare the subdomain stats, ordered alphabetically
        subdomain_stats = []
        for subdomain in sorted(self.subdomains.keys()):
            unique_page_count = len(self.subdomains[subdomain])  # Count unique pages
            subdomain_stats.append(f"http://{subdomain}.ics.uci.edu, {unique_page_count}")

        return subdomain_stats

# Singleton instance
global_stats = CrawlerStats()
