import logging
import sys

from concurrent import futures
from itertools import chain

import db

from scraper.mdr import scrape_mdr


def call_scraper(scraper):
    return scraper()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    db.init_database()

    executor = futures.ProcessPoolExecutor()

    scraper = [scrape_mdr]
    streams = chain.from_iterable(executor.map(call_scraper, scraper))

    db.insert_streams(streams)
    db.export_streams(sys.stdout)
