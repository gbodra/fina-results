import utils
import requests
import pandas as pd
from tqdm import tqdm

# PAGE_SIZE = 100
# BASE_URL = 'https://api.fina.org/fina/competitions'
# DISCIPLINE = 'SW'
# GROUP = 'FINA'
# COLS_REMOVE = ['metadata.watchNowURL', 'metadata.showGMSLogin', 'metadata.ticketsURL']
# EVENT_COLS = ['Id', 'OfficialName', 'Name', 'City', 'CountryCode', 'CountryId', 'CountryName', 'EventTypeName',
#               'EventTypeId', 'OdfCompetitionCode', 'PoolConfiguration', 'QualificationForEventTypes', 'RegionName',
#               'ResultsReceived', 'Series', 'Sports', 'From', 'To', 'TimeZone']


def crawl_competitions(cfg):
    page = 0
    dfs = []

    pbar = tqdm(total=1)

    while True:
        querystring = '?page=' + str(page) + '&pageSize=' + str(cfg['PAGE_SIZE']) + '&disciplines=' +\
                      cfg['DISCIPLINE'] + '&group=' + cfg['GROUP'] + '&sort=dateFrom,asc'

        data = utils.get_data_api_json(cfg['BASE_URL'] + querystring)
        df = utils.remove_cols(pd.json_normalize(data['content']), cfg['COLS_REMOVE'])
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
    utils.save_to_parquet(competitions_df, './data/', 'competitions.parquet')


def get_event_details(base_url, base_path, filename):
    df = pd.read_parquet(base_path + filename, columns=['id'])
    dfs = []
    pbar = tqdm(total=df.shape[0])

    for _, row in df.iterrows():
        url = base_url.format(row['id'])
        df = pd.json_normalize(utils.get_data_api_json(url))
        df = utils.filter_cols(df, EVENT_COLS)
        dfs.append(df)
        pbar.update(1)

    pbar.close()

    if utils.same_columns(dfs):
        events_details = pd.concat(dfs)
        events_details.reset_index(inplace=True, drop=True)
        utils.save_to_parquet(events_details, './data/', 'events_details.parquet')
