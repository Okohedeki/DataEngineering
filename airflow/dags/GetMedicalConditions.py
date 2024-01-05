import requests
from bs4 import BeautifulSoup
from collections import namedtuple
from PostgresConnector import PostgresConnector


class ICD10DataScraper:
    def __init__(self, start_url):
        self.start_url = start_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        }

    def get_page_content(self):
        response = requests.get(self.start_url, headers=self.headers)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')

    def scrape(self):
            DataTuple = namedtuple('DataTuple', 'code url description')
            soup = self.get_page_content()
            li_elements = soup.find_all('li')

            data = []
            for li in li_elements:
                a_tag = li.find('a', class_='identifier')
                div_tag = li.text

                if a_tag:
                    a_text = a_tag.text.strip()
                    a_href = requests.compat.urljoin(self.start_url, a_tag['href'])
                    div_text = div_tag.strip()

                    data_tuple = DataTuple(a_text, a_href, div_text)
                    data.append(data_tuple)

                    # Scrape the linked page
                    linked_page_soup = self.get_page_content_from_url(a_href)
                    linked_li_elements = linked_page_soup.find_all('li')
                    for linked_li in linked_li_elements:
                        linked_a_tag = linked_li.find('a', class_='identifier')
                        linked_div_tag = linked_li.text

                        if linked_a_tag:
                            linked_a_text = linked_a_tag.text.strip()
                            linked_a_href = requests.compat.urljoin(a_href, linked_a_tag['href'])
                            linked_div_text = linked_div_tag.strip()

                            linked_data_tuple = DataTuple(linked_a_text, linked_a_href, linked_div_text.replace('.', ''))
                            data.append(linked_data_tuple)

            return data

    def get_page_content_from_url(self, url):
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
# Usage
if __name__ == '__main__':
    start_url = "https://www.icd10data.com/ICD10CM/Codes"
    scraper = ICD10DataScraper(start_url)
    scraped_data = scraper.scrape()

    print(scraped_data)

    inserter = PostgresConnector(["code"])
    inserter.connect()

    # Insert the scraped data
    inserter.insert_batch_data('icd10_codes', scraped_data, page_size=100)
    inserter.close_connection()