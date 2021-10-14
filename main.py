import os
import crawler as cw

CACHE = os.environ.get('CACHE') == 'True'

if not CACHE:
    cw.crawl_competitions()
    cw.get_event_details('https://api.fina.org/fina/competitions/{}/events', './data/', 'competitions.parquet')

df_disciplines = cw.get_disciplines('./data/', 'events_details.parquet')
df_heats = cw.get_heats(df_disciplines, 'https://api.fina.org/fina/events/{}')
print(df_heats.head())
