import utils
import requests
import pandas as pd
from tqdm import tqdm

PAGE_SIZE = 100
BASE_URL = 'https://api.fina.org/fina/competitions'
DISCIPLINE = 'SW'
GROUP = 'FINA'
COLS_REMOVE = ['metadata.watchNowURL', 'metadata.showGMSLogin', 'metadata.ticketsURL']
EVENT_COLS = ['Id', 'OfficialName', 'Name', 'City', 'CountryCode', 'CountryId', 'CountryName', 'EventTypeName',
              'EventTypeId', 'OdfCompetitionCode', 'PoolConfiguration', 'QualificationForEventTypes', 'RegionName',
              'ResultsReceived', 'Series', 'Sports', 'From', 'To', 'TimeZone']


def get_data_fina(url):
    result = requests.get(url).json()
    return result


def crawl_competitions():
    page = 0
    dfs = []

    pbar = tqdm(total=1)

    while True:
        querystring = '?page=' + str(page) + '&pageSize=' + str(PAGE_SIZE) + '&disciplines=' + DISCIPLINE + \
                      '&group=' + GROUP + '&sort=dateFrom,asc'
        data = get_data_fina(BASE_URL + querystring)
        df = utils.remove_cols(pd.json_normalize(data['content']), COLS_REMOVE)
        dfs.append(df)

        if page == 0:
            pbar.total = int(data['pageInfo']['numPages'])
            pbar.refresh()

        pbar.update(1)
        page += 1
        if page >= data['pageInfo']['numPages']:
            break

    pbar.close()

    competitions_df = pd.concat(dfs)
    competitions_df.reset_index(inplace=True, drop=True)
    utils.save_to_feather(competitions_df, './data/', 'competitions.feather')


def get_event_details(base_url, base_path, filename):
    df = pd.read_feather(base_path + filename, columns=['id'])
    dfs = []
    pbar = tqdm(total=df.shape[0])

    for _, row in df.iterrows():
        url = base_url.format(row['id'])
        df = pd.json_normalize(get_data_fina(url))
        df = utils.filter_cols(df, EVENT_COLS)
        dfs.append(df)
        pbar.update(1)

    pbar.close()

    if utils.same_columns(dfs):
        events_details = pd.concat(dfs)
        events_details.reset_index(inplace=True, drop=True)
        utils.save_to_feather(events_details, './data/', 'events_details.feather')
