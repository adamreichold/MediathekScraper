import csv
import sqlite3


get_channels_statement = 'SELECT DISTINCT(channel) FROM streams'
get_topics_statement = 'SELECT DISTINCT(topic) FROM streams WHERE channel = ?'
shortest_url_for_channel_statement = 'SELECT url_{0} FROM streams WHERE channel = ? AND url_{0} IS NOT NULL ORDER BY url_{0} LIMIT 1'
shortest_url_for_topic_statement = 'SELECT url_{0} FROM streams WHERE channel = ? AND topic = ? AND url_{0} IS NOT NULL ORDER BY url_{0} LIMIT 1'
url_prefix_for_channel_statement = 'SELECT rowid FROM streams WHERE channel = ? AND url_{0} IS NOT NULL AND url_{0} NOT GLOB ? LIMIT 1'
url_prefix_for_topic_statement = 'SELECT rowid FROM streams WHERE channel = ? AND topic = ? AND url_{0} IS NOT NULL AND url_{0} NOT GLOB ? LIMIT 1'
insert_streams_statement = 'INSERT OR IGNORE INTO streams VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
get_streams_for_csv_statement = 'SELECT channel, topic, title, date, time, duration, description, url_web, url_large, url_medium, url_small FROM streams ORDER BY channel, topic, title'

connection = None


def get_channels(cursor):
    cursor.execute(get_channels_statement)
    return [row[0] for row in cursor.fetchall()]


def get_topics(cursor, channel):
    cursor.execute(get_topics_statement, (channel,))
    return [row[0] for row in cursor.fetchall()]


def get_shortest_url_for_channel(cursor, channel):
    url = []
    
    for kind in ('large', 'medium', 'small'):            
        cursor.execute(shortest_url_for_channel_statement.format(kind), (channel,))
        row = cursor.fetchone()
        if row:
            url.append(row[0])
            
    return min(url, key=len)


def get_url_prefix_for_channel(cursor, channel):
    prefix = get_shortest_url_for_channel(cursor, channel)
    
    while len(prefix) > 0:
        pattern = prefix + '*'

        for kind in ('large', 'medium', 'small'):
            cursor.execute(url_prefix_for_channel_statement.format(kind), (channel, pattern))
            if cursor.fetchone():
                prefix = prefix[:-1]
                break
        else:
            break

    return prefix


def get_shortest_url_for_topic(cursor, channel, topic):
    url = []

    for kind in ('large', 'medium', 'small'):            
        cursor.execute(shortest_url_for_topic_statement.format(kind), (channel, topic))
        row = cursor.fetchone()
        if row:
            url.append(row[0])
            
    return min(url, key=len)


def get_url_prefix_for_topic(cursor, channel, topic):
    prefix = get_shortest_url_for_topic(cursor, channel, topic)
        
    while len(prefix) > 0:
        pattern = prefix + '*'

        for kind in ('large', 'medium', 'small'):
            cursor.execute(url_prefix_for_topic_statement.format(kind), (channel, topic, pattern))
            if cursor.fetchone():
                prefix = prefix[:-1]
                break
        else:
            break

    return prefix


def get_shortest_url_web_for_channel(cursor, channel):
    cursor.execute(shortest_url_for_channel_statement.format('web'), (channel,))
    return cursor.fetchone()[0]


def get_url_web_prefix_for_channel(cursor, channel):
    prefix = get_shortest_url_web_for_channel(cursor, channel)

    while len(prefix) > 0:
        pattern = prefix + '*'

        cursor.execute(url_prefix_for_channel_statement.format('web'), (channel, pattern))
        if cursor.fetchone():
            prefix = prefix[:-1]
            continue

        break

    return prefix


def get_shortest_url_web_for_topic(cursor, channel, topic):
    cursor.execute(shortest_url_for_topic_statement.format('web'), (channel, topic))
    return cursor.fetchone()[0]


def get_url_web_prefix_for_topic(cursor, channel, topic):
    prefix = get_shortest_url_web_for_topic(cursor, channel, topic)

    while len(prefix) > 0:
        pattern = prefix + '*'

        cursor.execute(url_prefix_for_topic_statement.format('web'), (channel, topic, pattern))
        if cursor.fetchone():
            prefix = prefix[:-1]
            continue

        break

    return prefix


def get_url_prefixes(cursor):
    prefixes = {}

    for channel in get_channels(cursor):
        prefixes[channel] = get_url_prefix_for_channel(cursor, channel)

        for topic in get_topics(cursor, channel):
            prefixes[(channel, topic)] = get_url_prefix_for_topic(cursor, channel, topic)

    return prefixes

def get_url_web_prefixes(cursor):
    prefixes = {}

    for channel in get_channels(cursor):
        prefixes[channel] = get_url_web_prefix_for_channel(cursor, channel)

        for topic in get_topics(cursor, channel):
            prefixes[(channel, topic)] = get_url_web_prefix_for_topic(cursor, channel, topic)

    return prefixes
    

def init_database():
    global connection
    
    connection = sqlite3.connect(':memory:')
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE streams (url_scraped TEXT PRIMARY KEY, channel TEXT, topic TEXT, title TEXT, date TEXT, time TEXT, duration TEXT, description TEXT, url_web TEXT, url_large TEXT, url_medium TEXT, url_small TEXT)')
    cursor.execute('CREATE INDEX by_channel_topic_title ON streams (channel, topic, title)')
    cursor.execute('CREATE INDEX by_channel_url_large ON streams (channel, url_large)')
    cursor.execute('CREATE INDEX by_channel_topic_url_large ON streams (channel, topic, url_large)')
    cursor.execute('CREATE INDEX by_channel_url_medium ON streams (channel, url_medium)')
    cursor.execute('CREATE INDEX by_channel_topic_url_medium ON streams (channel, topic, url_medium)')
    cursor.execute('CREATE INDEX by_channel_url_small ON streams (channel, url_small)')
    cursor.execute('CREATE INDEX by_channel_topic_url_small ON streams (channel, topic, url_small)')
    cursor.execute('CREATE INDEX by_channel_url_web ON streams (channel, url_web)')
    cursor.execute('CREATE INDEX by_channel_topic_url_web ON streams (channel, topic, url_web)')


def insert_streams(streams):
    global connection
    
    cursor = connection.cursor()
    
    try:
        cursor.executemany(insert_streams_statement, streams)
        connection.commit()
    except:
        connection.rollback()
        raise
    
    cursor.execute('ANALYZE')


def export_streams(file):
    global connection

    cursor = connection.cursor()

    def keep_unique_values(value, curr_value):
        if value == curr_value:
            return ('', curr_value)
        else:
            return (value, value)
    
    curr_channel = None
    curr_topic = None
    curr_title = None

    prefixes = get_url_prefixes(cursor)
    curr_channel_prefix = None
    curr_topic_prefix = None

    def strip_channel_prefix(url):
        return url[len(curr_channel_prefix):] if url else url

    def strip_topic_prefix(url):
        return url[len(curr_channel_prefix) + len(curr_topic_prefix):] if url else url

    web_prefixes = get_url_web_prefixes(cursor)
    curr_channel_web_prefix = None
    curr_topic_web_prefix = None

    def strip_channel_web_prefix(url):
        return url[len(curr_channel_web_prefix):] if url else url

    def strip_topic_web_prefix(url):
        return url[len(curr_channel_web_prefix) + len(curr_topic_web_prefix):] if url else url

    writer = csv.writer(file)

    cursor.execute(get_streams_for_csv_statement)
    for row in cursor:
        channel, curr_channel = keep_unique_values(row[0], curr_channel)
        topic, curr_topic = keep_unique_values(row[1], curr_topic)

        channel_prefix, curr_channel_prefix = keep_unique_values(prefixes[curr_channel], curr_channel_prefix)
        topic_prefix, curr_topic_prefix = keep_unique_values(prefixes[(curr_channel, curr_topic)], curr_topic_prefix)

        topic_prefix = strip_channel_prefix(topic_prefix)

        channel_web_prefix, curr_channel_web_prefix = keep_unique_values(web_prefixes[curr_channel], curr_channel_web_prefix)
        topic_web_prefix, curr_topic_web_prefix = keep_unique_values(web_prefixes[(curr_channel, curr_topic)], curr_topic_web_prefix)

        topic_web_prefix = strip_channel_web_prefix(topic_web_prefix)

        title, curr_title = keep_unique_values(row[2], curr_title)

        date = row[3]
        time = row[4]
        duration = row[5]
        description = row[6]
        
        url_web = strip_topic_web_prefix(row[7])

        url_large = strip_topic_prefix(row[8])            
        url_medium = strip_topic_prefix(row[9])            
        url_small = strip_topic_prefix(row[10])

        writer.writerow((channel, channel_prefix, channel_web_prefix, topic, topic_prefix, topic_web_prefix, title, date, time, duration, description, url_web, url_large, url_medium, url_small))
