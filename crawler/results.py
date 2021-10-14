import utils
import pandas as pd
from tqdm import tqdm


def get_disciplines(base_path, filename):
    df = utils.read_file(base_path, filename)
    disciplines = []

    for _, row in tqdm(df.iterrows()):
        for sports_row in row.Sports:
            if sports_row is not None and sports_row['Code'] == 'SW' and len(sports_row['DisciplineList']) > 0:
                disciplines.append(sports_row['DisciplineList'])

    disciplines = utils.flatten_list(disciplines)
    return pd.DataFrame(disciplines)


# def get_event_results(df, url):
#


def get_heats(df, url):
    for _, row in tqdm(df.iterrows()):
        heats.append(row['Id'])

    heats = utils.flatten_list(heats)
    return pd.DataFrame(heats)

