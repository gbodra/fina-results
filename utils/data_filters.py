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