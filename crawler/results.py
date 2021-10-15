import utils
import pandas as pd
from tqdm import tqdm


def get_disciplines(base_path, filename):
    df = utils.read_file(base_path, filename)
    disciplines = []

    pbar = tqdm(total=df.shape[0])
    for _, row in df.iterrows():
        for sports_row in row.Sports:
            if sports_row is not None and sports_row['Code'] == 'SW' and len(sports_row['DisciplineList']) > 0:
                disciplines.append(sports_row['DisciplineList'])
        pbar.update(1)

    pbar.close()
    disciplines = utils.flatten_list(disciplines)
    return pd.DataFrame(disciplines)


def get_discipline_details(df, url):
    heats = []
    pbar = tqdm(total=df.shape[0])
    for _, row in df.iterrows():
        data_json = utils.get_data_api_json(url.format(row['Id']))
        heats.append(utils.json_to_pandas(data_json))
        pbar.update(1)

    pbar.close()
    return pd.concat(heats)

