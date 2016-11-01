import logging
import subprocess

from concurrent import futures
from datetime import datetime
from itertools import chain
from os import path

from dottorrent import Torrent

import db

from scraper.mdr import scrape_mdr


csv_dir = '/var/tmp/csv'
www_dir = '/var/tmp/www'
qbt_dir = '/var/tmp/qbt'

tracker_url = 'http://localhost:9000/announce'


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

    file_name = path.join(csv_dir, 'mediathek-{:%Y%m%d-%H%M%S}.csv'.format(today))

    with open(file_name, mode='w', newline='') as csv_file:
        db.export_streams(csv_file)

    # Compress CSV file using XZ
    subprocess.check_call(['xz', '-z', file_name])

    file_name += '.xz'

    logging.info("Exported streams to '%s'.", file_name)

    # Create torrent of compressed CSV file
    torrent = Torrent(path=file_name, trackers=[tracker_url], creation_date=today)
    torrent.generate()

    file_name += '.torrent'

    with open(file_name, mode='wb') as torrent_file:
        torrent.save(torrent_file)

    logging.info("Created torrent at '%s'.", file_name)

    # Publish torrent via HTTP and qBt
    subprocess.check_call(['ln', '-f', file_name, path.join(www_dir, 'mediathek.csv.xz.torrent')])
    subprocess.check_call(['ln', file_name, qbt_dir])
