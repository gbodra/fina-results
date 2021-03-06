import utils
import pandas as pd
from tqdm import tqdm
tqdm.pandas()


class Fina:
    def __init__(self, cache=False):
        self.cache = cache
        self.df_competitions = pd.DataFrame()
        self.df_events_details = pd.DataFrame()
        self.df_disciplines = pd.DataFrame()
        self.heats = pd.DataFrame()
        self.results = pd.DataFrame()
        self.config = utils.get_config()

    def get_results(self):
        print('Carregando competições...')
        self.__crawl_competitions()
        print('Carregando detalhes dos eventos...')
        self.__crawl_event_details()
        print('Carregando disciplinas...')
        self.__get_disciplines()
        print('Carregando detalhes das disciplinas...')
        self.__crawl_discipline_details()
        print('Carregando resultados...')
        self.__process_results()

        return self.results

    def __crawl_competitions(self):
        if self.cache:
            self.df_competitions = pd.DataFrame(utils.read_file(self.config['BASE_PATH'], 'competitions.parquet')['id'])
            return True

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
        self.df_competitions = pd.DataFrame(competitions_df['id'])
        utils.save_to_file(competitions_df, self.config['BASE_PATH'], 'competitions.parquet')

        return False

    def __crawl_event_details(self):
        if self.cache:
            self.df_events_details = utils.read_file(self.config['BASE_PATH'], 'events_details.parquet')
            return True

        dfs = []
        pbar = tqdm(total=self.df_competitions.shape[0])

        for _, row in self.df_competitions.iterrows():
            url = self.config['BASE_URL'] + 'competitions/{}/events'
            url = url.format(row['id'])
            df = pd.json_normalize(utils.get_data_api_json(url))
            df = utils.filter_cols(df, self.config['EVENT_COLS'])
            dfs.append(df)
            pbar.update(1)

        pbar.close()

        if utils.same_columns(dfs):
            events_details = pd.concat(dfs)
            self.df_events_details = events_details
            utils.save_to_file(events_details, self.config['BASE_PATH'], 'events_details.parquet')

        return False

    def __get_disciplines(self):
        disciplines = []

        pbar = tqdm(total=self.df_events_details.shape[0])
        for _, row in self.df_events_details.iterrows():
            for sports_row in row.Sports:
                if sports_row is not None and sports_row['Code'] == self.config['DISCIPLINE']\
                        and len(sports_row['DisciplineList']) > 0:
                    disciplines.append(sports_row['DisciplineList'])
            pbar.update(1)

        pbar.close()
        disciplines = utils.flatten_list(disciplines)
        self.df_disciplines = pd.DataFrame(disciplines)

    def __crawl_discipline_details(self):
        if self.cache:
            self.heats = utils.read_file(self.config['BASE_PATH'], 'disciplines_details.parquet')
            return True

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
        utils.save_to_file(self.heats, self.config['BASE_PATH'], 'disciplines_details.parquet')

        return False

    def __process_results(self):
        heats_df = self.heats.explode('Heats')
        heats_df = pd.concat([heats_df, heats_df['Heats'].progress_apply(pd.Series)], axis=1)
        cols_remove = ['Comment', 'DisciplineStartDate', 'DisciplineStartTime', 'DisciplineEndDate',
                       'DisciplineEndTime', 'EventOfficialName', 'EventResultDate', 'EventResultTime', 'Heats',
                       'TimingAndScoringPartnerName', 'TimingAndScoringPartnerLogo1', 'TimingAndScoringPartnerLogo2',
                       'TimingAndScoringPartnerLogo3', 'TimingAndScoringPartnerLogo4', 'Id', 'LastChange', 0,
                       'EndDate', 'EndTime', 'EndUtcDateTime', 'ExcludeFromEventSummary', 'Name', 'ObjectState',
                       'Time', 'UnitCode', 'UtcDateTime', 'DisciplineCode', 'AgeGroup', 'Distance', 'PhaseCode',
                       'PhaseId', 'ResultStatus']
        heats_df = utils.remove_cols(heats_df, cols_remove)
        heats_df = heats_df.loc[:, ~heats_df.columns.duplicated()]
        heats_df = heats_df.explode('Results')
        heats_df = pd.concat([heats_df, heats_df['Results'].progress_apply(pd.Series)], axis=1)

        cols_remove = [0, 'BiographyId', 'ClubCountryCode', 'ClubName', 'GmsId', 'ScoreboardPhoto', 'ScoringAbbr',
                       'SubResults', 'Results']
        heats_df = utils.remove_cols(heats_df, cols_remove)
        heats_df = heats_df.loc[:, ~heats_df.columns.duplicated()]
        heats_df['Time_Seconds'] = heats_df['Time'].progress_apply(lambda x: utils.to_seconds(str(x)))
        self.results = heats_df
        utils.save_to_file(self.results, self.config['BASE_PATH'], 'results.parquet')
