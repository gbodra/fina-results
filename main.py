import os
import utils
import crawler as cw

CACHE = os.environ.get('CACHE') == 'True'

if not CACHE:
    cw.crawl_competitions()
    cw.get_event_details('https://api.fina.org/fina/competitions/{}/events', './data/', 'competitions.parquet')
    df_disciplines = cw.get_disciplines('./data/', 'events_details.parquet')
    df_disciplines_details = cw.get_discipline_details(df_disciplines, 'https://api.fina.org/fina/events/{}')
    utils.save_to_file(df_disciplines_details, './data/', 'disciplines_details.parquet')

