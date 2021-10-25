import pandas as pd
from tqdm import tqdm


def remove_cols(df, col_names):
    for col in tqdm(col_names):
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    return df


def same_columns(dfs):
    list_cols = [list(dfs[0].columns) == list(df.columns) for df in dfs]
    if all(list_cols):
        return True
    else:
        return False


def filter_cols(df, col_names):
    return df.filter(col_names)


def flatten_list(t):
    return [item for sublist in t for item in sublist]


def json_to_pandas(data):
    return pd.json_normalize(data)


def to_seconds(result_time):
    if result_time == 'None':
        return 0

    time_split = result_time.split(':')
    total_seconds = 0

    if len(time_split) == 3:
        total_seconds = float(time_split[0]) * 60 * 60
        total_seconds += float(time_split[1]) * 60
        total_seconds += float(time_split[2])

        return total_seconds

    if len(time_split) == 2:
        total_seconds = float(time_split[0]) * 60
        total_seconds += float(time_split[1])

        return total_seconds

    return float(result_time)
