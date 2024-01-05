import requests
from bs4 import BeautifulSoup
from PostgresConnector import PostgresConnector
import polars as pl
from concurrent.futures import ThreadPoolExecutor
from collections import namedtuple
from itertools import chain


class DownloadArticleText:
    def __init__(self, url, pmid):
        self.url = url
        self.pmid = pmid
        self.title = None
        self.authors = []
        self.joi = None
        self.text = None

    def scrape(self):
        response = requests.get(self.url)
        print(self.url)
        if response.status_code == 200:
            self.parse_html(response.text)
        else:
            print("Failed to retrieve the webpage. Status code:", response.status_code)


    def parse_html(self, html):
        soup = BeautifulSoup(html, 'html.parser')

        title_element = soup.find('h1', class_='heading-title')
        self.title = title_element.text.strip() if title_element else ''

        self.authors = [author.text for author in soup.find_all('a', class_='full-name')] if soup.find_all('a', class_='full-name') else []

        joi_element = soup.find('a', class_='id-link')
        self.joi = joi_element.text.strip() if joi_element else ''

        abstract_content = soup.find('div', class_='abstract-content selected')
        self.text = ' '.join([p.text.strip() for p in abstract_content.find_all('p')]) if abstract_content else ''

    def display_results(self):
        print("PMID:", self.pmid)
        print("Title:", self.title)
        print(self.authors)
        print("JOI:", self.joi)
        print("Text:", self.text)
        print("Good")

    @staticmethod
    def scrape_and_collect(url, pmid):
        scraper = DownloadArticleText(url, pmid)
        scraper.scrape()
        scraped_data = namedtuple('ScrapedData', 'pmid title website_url authors joi text')
        return scraped_data(pmid, scraper.title, url, scraper.authors, scraper.joi, scraper.text)

if __name__ == '__main__':
    # Named tuple definitions
    PartialScrapedData = namedtuple('PartialScrapedData', ['title', 'website_url', 'abstract', 'pmid', 'joi'])
    Publications = namedtuple('Publications', ['pmid','authorid'])
    AuthorData = namedtuple('AuthorData', ['authorname'])


    # Initialize and configure the PostgresConnector
    inserter = PostgresConnector()
    inserter.set_unique_columns(['pmid'])
    inserter.connect()

    # Read the first row from 'pubmeddata' into a Polars DataFrame
    df = inserter.read_table_into_polars_dataframe('pubmeddata')
    df = df.slice(0, 1) if df is not None else None

    # Scrape and collect data if DataFrame is valid
    if isinstance(df, pl.DataFrame):
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda row: DownloadArticleText.scrape_and_collect(row[1], row[0]), df.rows()))

    # Insert scraped data into 'medicalpdfs'
    selected_data = [PartialScrapedData(item.title, item.website_url, item.text, item.pmid, item.joi) for item in results]
    inserter.insert_batch_data('medicalpdfs', selected_data, page_size=100)

    # Insert unique authors into 'authors'
    inserter.set_unique_columns(['authorname'])
    unique_authors = set(chain.from_iterable(item.authors for item in results))

    for author in unique_authors:
        try:
            author_data = AuthorData(authorname=author)
            inserter.insert_batch_data('authors', [author_data], page_size=100)
        except Exception as e:
            print(f"Error inserting author {author}: {e}")

    # Insert data into 'publications'
    inserter.set_unique_columns([])

    for item in results:
        for author in set(item.authors):
            try:
                author_id_df = inserter.read_table_column_into_polars_dataframe('authors', 'authorname', author)
                if author_id_df is not None and not author_id_df.is_empty():
                    author_id = author_id_df['authorid'][0]
                    inserter.insert_batch_data('publications', [Publications(item.pmid, author_id)], page_size=100)
            except Exception as e:
                print(f"Error inserting publication data for author {author}: {e}")

    # Close the database connection
    inserter.close_connection()