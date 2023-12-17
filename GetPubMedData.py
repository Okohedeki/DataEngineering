from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import polars as pl
import logging
import traceback


from DownloadArticleText import DownloadArticleText
from HealthTerms import HealthTermsScraper
from PostgresConnector import PostgresConnector
from GetPmids import GetPmids
from GetMedicalConditions import ICD10DataScraper


class MedicalDataWorkflow:
    def __init__(self):
        self.PartialScrapedData = namedtuple(
            'PartialScrapedData', ['title', 'website_url', 'abstract', 'pmid', 'joi'])
        self.Publications = namedtuple('Publications', ['pmid', 'authorid'])
        self.AuthorData = namedtuple('AuthorData', ['authorname'])

    def run_health_terms(self):
        scraper = HealthTermsScraper()
        scraped_data = scraper.scrape_terms()
        inserter = PostgresConnector()
        inserter.set_unique_columns(['term'])

        inserter.connect()
        inserter.insert_batch_data('healthterms', scraped_data, page_size=100)
        inserter.close_connection()

    def run_medical_conditions(self):
        start_url = "https://www.icd10data.com/ICD10CM/Codes"
        scraper = ICD10DataScraper(start_url)
        scraped_data = scraper.scrape()
        inserter = PostgresConnector()
        inserter.set_unique_columns(['code'])
        inserter.connect()
        inserter.insert_batch_data('icd10_codes', scraped_data, page_size=100)
        inserter.close_connection()

    def run_get_pmids(self):
        logging.basicConfig(level=logging.INFO)
        word_lists = HealthTermsScraper().scrape_terms()
        page_number = 1
        scraper = GetPmids(word_lists, page_number)
        pmids = scraper.get_pmids()
        inserter = PostgresConnector()
        #inserter.set_unique_columns(['term'])

        inserter.connect()
        inserter.insert_batch_data('pubmeddata', pmids, page_size=100)
        inserter.close_connection()

    def run_download_article_text(self):
        inserter = PostgresConnector()
        inserter.connect()
        df = inserter.read_table_into_polars_dataframe('pubmeddata')
        df = df.slice(0, 1) if df is not None else None
        if isinstance(df, pl.DataFrame):
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(
                    lambda row: DownloadArticleText.scrape_and_collect(row[1], row[0]), df.rows()))
            selected_data = [self.PartialScrapedData(
                item.title, item.website_url, item.text, item.pmid, item.joi) for item in results]
            inserter.insert_batch_data(
                'medicalpdfs', selected_data, page_size=100)
            inserter.set_unique_columns(['authorname'])
            unique_authors = set(chain.from_iterable(
                item.authors for item in results))
            for author in unique_authors:
                try:
                    author_data = self.AuthorData(authorname=author)
                    inserter.insert_batch_data(
                        'authors', [author_data], page_size=100)
                except Exception as e:
                    print(f"Error inserting author {author}: {e}")
                    traceback.print_exc()

            inserter.set_unique_columns([])
            for item in results:
                for author in set(item.authors):
                    try:
                        author_id_df = inserter.read_table_column_into_polars_dataframe(
                            'authors', 'authorname', author)
                        if author_id_df is not None and not author_id_df.is_empty():
                            author_id = author_id_df['authorid'][0]
                            inserter.insert_batch_data(
                                'publications', [self.Publications(item.pmid, author_id)], page_size=100)
                    except Exception as e:
                        print(f"Error inserting publication data for author {author}: {e}")
                        traceback.print_exc()

        inserter.close_connection()


if __name__ == '__main__':
    # Usage
    workflow = MedicalDataWorkflow()
    workflow.run_health_terms()
    workflow.run_medical_conditions()
    workflow.run_get_pmids()
    workflow.run_download_article_text()
