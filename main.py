import os
import utils
import crawler as cw

CACHE = os.environ.get('CACHE') == 'True'
fina = cw.Fina()

if not CACHE:
    fina.crawl_competitions()
    fina.crawl_event_details()
    fina.get_disciplines()
    fina.crawl_discipline_details()
    fina.get_results()
