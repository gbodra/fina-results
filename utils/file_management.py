import pandas as pd


def save_to_file(df, base_path, filename):
    """Save Pandas DataFrame to a file

        Parameters:
        filename (string): Name of the file to be saved
    """
    df.reset_index(inplace=True, drop=True)

    extension = filename.split('.')[1]
    if extension == 'parquet':
        df.to_parquet(base_path + filename)
    if extension == 'feather':
        df.to_feather(base_path + filename)


def read_file(base_path, filename):
    extension = filename.split('.')[1]
    if extension == 'parquet':
        return pd.read_parquet(base_path + filename)

    if extension == 'feather':
        return pd.read_feather(base_path + filename)
