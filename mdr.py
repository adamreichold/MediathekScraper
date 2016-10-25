import itertools
import json
import logging
import re
import requests
import sqlite3
import threading

from concurrent import futures
from lxml import etree, html


tls = threading.local()


def get_url(url):
    session = getattr(tls, 'session', None)

    if not session:
        session = requests.Session()
        setattr(tls, 'session', session)

    return session.get(url)


def add_prefix(url):
    prefix = 'http://www.mdr.de'

    if url.startswith(prefix):
        return url

    return prefix + url


def scrape_letters():
    try:
        url = 'http://www.mdr.de/mediathek/fernsehen/a-z/index.html'
        pattern = '/mediathek/fernsehen/a-z/sendungenabisz100_inheritancecontext-header_letter-'

        logging.info("Scraping letters from '%s'...", url)
        page = get_url(url)
        page = html.fromstring(page.content)

        letters = page.xpath("//a[contains(@href,'%s')]" % pattern)

        return map(lambda letter: add_prefix(letter.get('href')), letters)
    except Exception as exception:
        logging.critical("Failed to scrape letters from '%s'.", url)

        return []


def scrape_days():
    try:
        url = 'http://www.mdr.de/mediathek/fernsehen/index.html'
        pattern = '/mediathek/fernsehen/sendung-verpasst--100_date-'

        logging.info("Scraping days from '%s'...", url)
        page = get_url(url)
        page = html.fromstring(page.content)

        days = page.xpath("//a[contains(@href,'%s')]" % pattern)

        return map(lambda day: add_prefix(day.get('href')), days)
    except Exception as exception:
        logging.critical("Failed to scrape days from '%s'.", url)

        return []


def scrape_shows(url):
    try:
        pattern = '/mediathek/fernsehen/'

        logging.info("Scraping shows from '%s'...", url)
        page = get_url(url)
        page = html.fromstring(page.content)

        shows = page.xpath("//div[@id='content']//div[@class='shortInfos']")

        result = []

        for show in shows:
            headline = show.xpath("./descendant::a[@class='headline' and contains(@href, '%s')]" % pattern)
            if headline:
                headline = headline[0]
            else:
                continue

            subtitle = show.xpath("./descendant::p[@class='subtitle']/a")
            if subtitle:
                subtitle = ' - ' + subtitle[0].text
            else:
                subtitle = ''

            result.append((headline.text + subtitle, add_prefix(headline.get('href'))))

        return result
    except Exception as exception:
        logging.exception("Failed to scrape shows from '%s'.", url)

        return []


def scrape_broadcasts(show, url):
    try:
        pattern = '/mediathek/fernsehen/'

        logging.info("Scraping broadcasts from '%s'...", url)
        page = get_url(url)
        page = html.fromstring(page.content)

        broadcasts = page.xpath("//div[@id='content']//div[@class='shortInfos']")

        result = []

        for broadcast in broadcasts:
            headline = broadcast.xpath("./descendant::a[@class='headline' and contains(@href, '%s')]" % pattern)
            if headline:
                headline = headline[0]
            else:
                continue

            subtitle = broadcast.xpath("./descendant::p[@class='subtitle']")
            if subtitle:
                subtitle = ' - ' + subtitle[0].text
            else:
                subtitle = ''

            result.append((show, headline.text + subtitle, add_prefix(headline.get('href'))))

        return result
    except Exception as exception:
        logging.exception("Failed to scrape broadcasts from '%s'.", url)

        return []


def scrape_streams(show, title, url):
    try:
        logging.info("Scraping streams from '%s'...", url)
        page = get_url(url)
        page = html.fromstring(page.content)

        player = page.xpath("//div[@class='mediaCon ' and contains(@data-ctrl-player, 'playerXml')]")[0]
        player = player.get('data-ctrl-player')
        player = player.replace("'", '"')
        player = json.loads(player)
        player = add_prefix(player['playerXml'])

        logging.info("Getting streams from '%s'...", url)
        data = get_url(player)
        data = etree.fromstring(data.content)

        # TODO: Parse meta-data like title, description, date and time

        assets = data.xpath("//assets/asset[profileName and progressiveDownloadUrl]")

        urls = {}

        for asset in assets:
            name = asset.findtext('profileName')
            url = asset.findtext('progressiveDownloadUrl')

            match = re.search(r'\| MP4 Web (\w+\+?) \|', name)
            if match:
                urls[match.group(1)] = url

        return [(show, title, url, urls)]
    except Exception as exception:
        logging.exception("Failed to scrape streams from '%s'.", url)

        return []


def make_row(stream):
    try:
        topic, title, web_url, urls = stream

        url = urls.get('L+', urls.get('L', ''))
        url_large = urls.get('XL', '')
        url_small = urls.get('M', '')

        return [('MDR', topic, title, web_url, url, url_large, url_small)]
    except Exception as exception:
        logging.exception("Failed to convert stream descriptor to database row.")

        return []


def main():
    executor = futures.ThreadPoolExecutor(max_workers=1)

    letters_and_days = itertools.chain(executor.submit(scrape_letters).result(), executor.submit(scrape_days).result())
    shows = itertools.chain.from_iterable(executor.map(scrape_shows, letters_and_days))
    broadcasts = itertools.chain.from_iterable(executor.map(lambda show: scrape_broadcasts(*show), shows))
    streams = itertools.chain.from_iterable(executor.map(lambda broadcast: scrape_streams(*broadcast), broadcasts))

    connection = sqlite3.connect(':memory:')
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE broadcasts (channel TEXT, topic TEXT, title TEXT, web_url TEXT, url TEXT, url_large TEXT, url_small TEXT)')

    rows = itertools.chain.from_iterable(map(make_row, streams))
    cursor.executemany('INSERT INTO broadcasts VALUES (?, ?, ?, ?, ?, ?, ?)', rows)

    connection.commit()

    cursor.execute('SELECT COUNT(*) FROM broadcasts')
    row_count = cursor.fetchone()[0]

    logging.info('Inserted %d rows into database...', row_count)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    main()
