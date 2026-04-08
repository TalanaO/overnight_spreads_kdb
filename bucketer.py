import pandas as pd
import numpy as np


BUCKET_FREQS = ['1min', '5min', '15min']


def bucket_spreads(df: pd.DataFrame, freq: str = '5min') -> pd.DataFrame:
    """
    Aggregates spread data into fixed time buckets.

    Groups quotes by sym, date, and time bucket, computing spread
    statistics within each window.

    Parameters
    ----------
    df    : pd.DataFrame  Output of calculator.calculate_spreads()
    freq  : str           Pandas offset alias: '1min', '5min', '15min'

    Returns
    -------
    pd.DataFrame
        One row per (sym, date, bucket) with avg/median/max/std/count
    """
    df = df.copy()
    df = df.set_index('timestamp')

    bucketed = (
        df.groupby(['sym', 'date', pd.Grouper(freq=freq)])['spread']
        .agg(
            avg    = 'mean',
            median = 'median',
            max    = 'max',
            std    = 'std',
            count  = 'count',
        )
        .reset_index()
        .rename(columns={'timestamp': 'bucket'})
        .dropna(subset=['avg'])   # drop empty buckets
    )

    return bucketed


def bucket_all(df: pd.DataFrame) -> dict:
    """
    Runs bucketing at all three time resolutions.

    Returns
    -------
    dict with keys '1min', '5min', '15min', each a DataFrame
    """
    return {freq: bucket_spreads(df, freq) for freq in BUCKET_FREQS}


if __name__ == '__main__':
    from mock_data  import generate_mock_data
    from cleaner    import clean_quotes
    from calculator import calculate_spreads

    df      = generate_mock_data('2026-04-08', '2026-04-07')
    df      = clean_quotes(df)
    df      = calculate_spreads(df)
    buckets = bucket_all(df)

    for freq, bdf in buckets.items():
        print(f"\n{freq} buckets — shape: {bdf.shape}")
        print(bdf[bdf['sym'] == 'EURUSD'].head(5).to_string(index=False))
