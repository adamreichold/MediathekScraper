import logging
import subprocess

from concurrent import futures
from datetime import datetime
from itertools import chain

from dottorrent import Torrent

import db

from scraper.mdr import scrape_mdr


def call_scraper(scraper):
    return scraper()


def dummy_scraper():
    return []


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    db.init_database()

    executor = futures.ProcessPoolExecutor()

    scraper = [dummy_scraper, scrape_mdr]
    streams = chain.from_iterable(executor.map(call_scraper, scraper))

    db.insert_streams(streams)

    # Export streams to CSV file
    today = datetime.today()

    file_name = '/var/tmp/csv/mediathek-{:%Y%m%d-%H%M%S}.csv'.format(today)

    with open(file_name, mode='w', newline='') as csv_file:
        db.export_streams(csv_file)

    # Compress CSV file using XZ
    subprocess.check_call(['xz', '-z', file_name])

    file_name += '.xz'

    logging.info("Exported streams to '%s'.", file_name)

    # Create torrent of compressed CSV file
    torrent = Torrent(path=file_name, trackers=['http://localhost:9000/announce'], creation_date=today)
    torrent.generate()

    file_name += '.torrent'

    with open(file_name, mode='wb') as torrent_file:
        torrent.save(torrent_file)

    subprocess.check_call(['ln', file_name, '/var/tmp/qbt'])

    subprocess.check_call(['ln', '-f', file_name, '/var/tmp/www/mediathek.csv.xz.torrent'])

    logging.info("Created torrent at '%s'.", file_name)
