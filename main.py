import os
import crawler as cw

CACHE = os.environ.get('CACHE') == 'True'

if not CACHE:
    cw.crawl_competitions()
    cw.get_event_details('https://api.fina.org/fina/competitions/{}/events', './data/', 'competitions.feather')
