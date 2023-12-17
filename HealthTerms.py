import requests
from bs4 import BeautifulSoup
from collections import namedtuple
from PostgresConnector import PostgresConnector

class HealthTermsScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        self.TermTuple = namedtuple('TermTuple', 'term definition')
        self.urls = [
        "https://www.health.harvard.edu/a-through-c#A-terms",
        "https://www.health.harvard.edu/d-through-i#D-terms",
        "https://www.health.harvard.edu/j-through-p#O-terms",
        "https://www.health.harvard.edu/q-through-z#Q-terms"
    ]

    def get_page_content(self, url_to_use):
        response = requests.get(url_to_use, headers=self.headers)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')

    def scrape_terms(self):
        all_terms = []
        for url in self.urls:
            soup = self.get_page_content(url)
            div_content = soup.find('div', class_='max-w-md-lg mx-auto')
            if not div_content:
                continue

            p_tags = div_content.find_all('p')
            for p in p_tags:
                if ':' in p.text:
                    term, definition = p.text.split(':', 1)
                    term_tuple = self.TermTuple(term.strip(), definition.strip())

                    if term != 'Browse dictionary by letter':
                        all_terms.append(term_tuple)

        return all_terms

# Usage
if __name__ == '__main__':

    scraper = HealthTermsScraper()
    scraped_data = scraper.scrape_terms()

    print(scraped_data)

    inserter = PostgresConnector(["term"])
    inserter.connect()

    # Insert the scraped data
    inserter.insert_batch_data('healthterms', scraped_data, page_size=100)
    inserter.close_connection()