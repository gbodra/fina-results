import utils
import pandas as pd
from tqdm import tqdm


class Fina:
    def __init__(self, cache=False):
        self.cache = cache
        self.df_disciplines = pd.DataFrame()
        self.heats = pd.DataFrame()
        self.results = pd.DataFrame()
        self.config = utils.get_config()

    def get_results(self):
        if not self.cache:
            self.crawl_competitions()
            self.crawl_event_details()
            self.get_disciplines()
            self.crawl_discipline_details()
            self.process_results()

        return utils.read_file(self.config['BASE_PATH'], 'results.parquet')

    def crawl_competitions(self):
        page = 0
        dfs = []

        pbar = tqdm(total=1)

        while True:
            querystring = '?page=' + str(page) + '&pageSize=' + str(self.config['PAGE_SIZE']) + '&disciplines=' + \
                          self.config['DISCIPLINE'] + '&group=' + self.config['GROUP'] + '&sort=dateFrom,asc'

            data = utils.get_data_api_json(self.config['BASE_URL'] + 'competitions' + querystring)
            df = utils.remove_cols(pd.json_normalize(data['content']), self.config['COLS_REMOVE'])
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
        utils.save_to_file(competitions_df, self.config['BASE_PATH'] + 'competitions.parquet')

    def crawl_event_details(self):
        df = pd.read_parquet(self.config['BASE_PATH'] + 'competitions.parquet', columns=['id'])
        dfs = []
        pbar = tqdm(total=df.shape[0])

        for _, row in df.iterrows():
            url = self.config['BASE_URL'] + 'competitions/{}/events'
            url = url.format(row['id'])
            df = pd.json_normalize(utils.get_data_api_json(url))
            df = utils.filter_cols(df, self.config['EVENT_COLS'])
            dfs.append(df)
            pbar.update(1)

        pbar.close()

        if utils.same_columns(dfs):
            events_details = pd.concat(dfs)
            utils.save_to_file(events_details, self.config['BASE_PATH'] + 'events_details.parquet')

    def get_disciplines(self):
        df = utils.read_file(self.config['BASE_PATH'], 'events_details.parquet')
        disciplines = []

        pbar = tqdm(total=df.shape[0])
        for _, row in df.iterrows():
            for sports_row in row.Sports:
                if sports_row is not None and sports_row['Code'] == self.config['DISCIPLINE']\
                        and len(sports_row['DisciplineList']) > 0:
                    disciplines.append(sports_row['DisciplineList'])
            pbar.update(1)

        pbar.close()
        disciplines = utils.flatten_list(disciplines)
        self.df_disciplines = pd.DataFrame(disciplines)

    def crawl_discipline_details(self):
        heats = []
        pbar = tqdm(total=self.df_disciplines.shape[0])
        for _, row in self.df_disciplines.iterrows():
            url = self.config['BASE_URL'] + 'events/{}'
            url = url.format(row['Id'])
            data_json = utils.get_data_api_json(url)
            heats.append(utils.json_to_pandas(data_json))
            pbar.update(1)

        pbar.close()
        self.heats = pd.concat(heats)
        utils.save_to_file(self.heats, './data/disciplines_details.parquet')

    def process_results(self):
        disciplines_details = utils.read_file(self.config['BASE_PATH'], 'disciplines_details.parquet')
        results = []
        pbar = tqdm(total=disciplines_details.shape[0])
        for _, row in disciplines_details.iterrows():
            for heat in row['Heats']:
                for result in heat['Results']:
                    results.append(result)

            pbar.update(1)

        self.results = pd.DataFrame(results)
        utils.save_to_file(self.results, 'results.parquet')
