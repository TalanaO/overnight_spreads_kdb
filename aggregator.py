import pandas as pd
import numpy as np


def aggregate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes per-pair, per-day summary statistics on spread.

    Input requires columns: sym, date, spread, spread_pips, relative_spread

    Returns one row per (sym, date) with columns:
        avg_spread, median_spread, max_spread, std_spread,
        avg_spread_pips, avg_relative_spread, quote_count

    Parameters
    ----------
    df : pd.DataFrame
        Output of calculator.calculate_spreads()

    Returns
    -------
    pd.DataFrame
        Aggregated metrics indexed by sym and date
    """
    agg = (
        df.groupby(['sym', 'date'])
        .agg(
            avg_spread        = ('spread',          'mean'),
            median_spread     = ('spread',          'median'),
            max_spread        = ('spread',          'max'),
            std_spread        = ('spread',          'std'),
            avg_spread_pips   = ('spread_pips',     'mean'),
            avg_relative_spread = ('relative_spread', 'mean'),
            quote_count       = ('spread',          'count'),
        )
        .reset_index()
    )

    return agg


if __name__ == '__main__':
    from mock_data   import generate_mock_data
    from cleaner     import clean_quotes
    from calculator  import calculate_spreads

    df  = generate_mock_data('2026-04-08', '2026-04-07')
    df  = clean_quotes(df)
    df  = calculate_spreads(df)
    agg = aggregate_metrics(df)

    print("Aggregated metrics (all pairs, both days):")
    print(agg.to_string(index=False))
