from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
from bs4 import BeautifulSoup
from CrawlerStats import global_stats


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                output_statistics_to_file()
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            # Process the response content and update statistics
            self.process_response(tbd_url, resp)

            scraped_urls = scraper.scraper(tbd_url, resp, self.config)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)

    def process_response(self, url, resp):
        # Ensure the response is valid and content is available.
        if not resp.raw_response or not resp.raw_response.content:
            self.logger.warning(f"No content found for {url}")
            return
        
        # Parse content with BeautifulSoup to extract text.
        try:
            soup = BeautifulSoup(resp.raw_response.content, 'lxml')
            text = soup.get_text(separator=' ', strip=True)
            
            # Update global statistics
            global_stats.update_page(url, text)
            self.logger.info(f"Successfully processed {url}")

        except Exception as e:
            self.logger.error(f"Error processing {url}: {str(e)}")

def output_statistics_to_file():
    """Gather crawler statistics and write them to a file."""
    # Get statistics from the CrawlerStats instance
    unique_page_count = global_stats.get_unique_page_count()
    longest_page = global_stats.get_longest_page()
    most_common_words = global_stats.get_most_common_words(n=50)
    subdomain_stats = global_stats.get_subdomain_stats()

    # Open file to write the statistics
    with open('crawler_output.txt', 'w') as f:
        # Write unique page count
        f.write(f"Unique Pages Crawled: {unique_page_count}\n\n")
        
        # Write longest page information
        if longest_page:
            f.write(f"Longest Page: {longest_page[0]} with {longest_page[1]} words\n\n")
        
        # Write most common words
        f.write("Most Common Words:\n")
        for word, count in most_common_words:
            f.write(f"{word}: {count}\n")
        f.write("\n")
        
        # Write subdomain statistics
        f.write("Subdomain Stats:\n")
        for subdomain_stat in subdomain_stats:
            f.write(f"{subdomain_stat}\n")

    print("Crawler statistics written to 'crawler_output.txt'")