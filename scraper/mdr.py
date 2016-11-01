import json
import logging
import re

from concurrent import futures
from itertools import chain
from lxml import etree, html

from net import get_url


def add_url_prefix(url):
    prefix = 'http://www.mdr.de'

    return url if url.startswith(prefix) else prefix + url


def scrape_letters():
    try:
        url = add_url_prefix('/mediathek/fernsehen/a-z/index.html')
        pattern = '/mediathek/fernsehen/a-z/sendungenabisz100_inheritancecontext-header_letter-'

        logging.info("Scraping letters from '%s'...", url)
        page = get_url(url)
        page = html.fromstring(page.content)

        letters = page.xpath("//a[contains(@href, '%s')]" % pattern)

        return map(lambda letter: add_url_prefix(letter.get('href')), letters)
    except Exception as exception:
        logging.critical("Failed to scrape letters from '%s'.", url)

        return []


def scrape_days():
    try:
        url = add_url_prefix('/mediathek/fernsehen/index.html')
        pattern = '/mediathek/fernsehen/sendung-verpasst--100_date-'

        logging.info("Scraping days from '%s'...", url)
        page = get_url(url)
        page = html.fromstring(page.content)

        days = page.xpath("//a[contains(@href, '%s')]" % pattern)

        return map(lambda day: add_url_prefix(day.get('href')), days)
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

            result.append((headline.text + subtitle, add_url_prefix(headline.get('href'))))

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

            result.append((show, headline.text + subtitle, add_url_prefix(headline.get('href'))))

        return result
    except Exception as exception:
        logging.exception("Failed to scrape broadcasts from '%s'.", url)

        return []


def scrape_streams(show, title, url):
    try:
        logging.info("Scraping streams from '%s'...", url)
        page = get_url(url)
        page = html.fromstring(page.content)

        player = page.xpath("//div[contains(@class, 'mediaCon') and contains(@data-ctrl-player, 'playerXml')]")[0]
        player = player.get('data-ctrl-player')
        player = player.replace("'", '"')
        player = json.loads(player)
        player = add_url_prefix(player['playerXml'])

        logging.info("Getting streams from '%s'...", player)
        data = get_url(player)
        data = etree.fromstring(data.content)

        duration = data.xpath("//duration/text()")[0]
        description = data.xpath("//teaserText/text()")[0]
        
        broadcast = data.xpath("//broadcast")[0]
        date, time = broadcast.findtext('broadcastStartDate').split()
        url_web = broadcast.findtext('broadcastURL')

        streams = {}

        assets = data.xpath("//assets/asset[profileName and progressiveDownloadUrl]")
        for asset in assets:
            name = asset.findtext('profileName')
            stream = asset.findtext('progressiveDownloadUrl')

            match = re.search(r'\| MP4 Web (\w+\+?) \|', name)
            if match:
                streams[match.group(1)] = stream

        url_large = streams.get('XL')
        url_medium = streams.get('L', streams.get('L+'))
        url_small = streams.get('M')
        
        return [(player, 'MDR', show, title, date, time, duration, description, url_web, url_large, url_medium, url_small)]
    except Exception as exception:
        logging.exception("Failed to scrape streams from '%s'.", url)

        return []

  
def scrape_mdr():
    executor = futures.ThreadPoolExecutor(max_workers=3)

    letters_and_days = chain(executor.submit(scrape_letters).result(), executor.submit(scrape_days).result())
    shows = chain.from_iterable(executor.map(scrape_shows, letters_and_days))
    broadcasts = chain.from_iterable(executor.map(lambda show: scrape_broadcasts(*show), shows))
    streams = chain.from_iterable(executor.map(lambda broadcast: scrape_streams(*broadcast), broadcasts))

    return list(streams)

if __name__ == '__main__':
    import csv
    import sys

    logging.basicConfig(level=logging.DEBUG)

    writer = csv.writer(sys.stdout)

    for stream in scrape_mdr():
        writer.writerow(stream)
