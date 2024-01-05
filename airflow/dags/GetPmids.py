import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from PostgresConnector import PostgresConnector
from HealthTerms import HealthTermsScraper  # Import the function
import logging
import time
from collections import namedtuple

class GetPmids:
    def __init__(self, word_lists, page_number):
        self.word_lists = word_lists
        self.page_number = page_number
        self.url_template = "https://pubmed.ncbi.nlm.nih.gov/?term={}&page={}"


    def generate_urls(self):
        for index, terms in enumerate(self.word_lists):
            if index <= 2:
            
                term = terms.term
                # Replace spaces with '%20' for URL encoding
                search_terms = str(term).replace(" ", "%20")
                url = self.url_template.format(search_terms, self.page_number)
                yield url

    def fetch_url(self, url):
        time.sleep(1)
        try:
            logging.info(f"Fetching URL: {url}")
            response = requests.get(url)
            if response.status_code == 200:
                logging.info(f"URL fetched successfully: {url}")
                return response.text, url
            else:
                logging.warning(f"Failed to fetch URL: {url}, Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Error fetching URL: {url}, {str(e)}")
        return None, url

    def get_pmids(self):
        PubMedData = namedtuple('PubMedData', 'pmid url term')
        urls = list(self.generate_urls())
        pmid_list = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            html_pages_and_urls = list(executor.map(self.fetch_url, urls))

        for index, (html_page, url) in enumerate(html_pages_and_urls):

            if html_page is not None:
                soup = BeautifulSoup(html_page, 'html.parser')
                docsum_pmids = soup.find_all(class_="docsum-pmid")
                for docsum_pmid in docsum_pmids:
                    pmid = docsum_pmid.text
                    pmid_url = 'https://pubmed.ncbi.nlm.nih.gov/' + docsum_pmid.text + '/'
                    term = url.split('?term=')[1].split('&page=')[0].replace('%20', ' ')

                    pmid_list.append(PubMedData(pmid, pmid_url, term))

        return pmid_list

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    word_lists = HealthTermsScraper().scrape_terms()
    page_number = 1
    scraper = GetPmids(word_lists, page_number)
    pmids = scraper.get_pmids()

    inserter = PostgresConnector()
    inserter.connect()

    # Insert the scraped data
    inserter.insert_batch_data('pubmeddata', pmids, page_size=100)
    inserter.close_connection()