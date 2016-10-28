import requests
import threading

tls = threading.local()


def get_url(url):
    session = getattr(tls, 'session', None)

    if not session:
        session = requests.Session()
        setattr(tls, 'session', session)

    return session.get(url)
