import pandas as pd
import numpy as np


def compare_days(agg: pd.DataFrame, today: str, yesterday: str) -> pd.DataFrame:
    """
    Aligns today vs yesterday metrics and computes change statistics.

    Parameters
    ----------
    agg       : pd.DataFrame  Output of aggregator.aggregate_metrics()
    today     : str           e.g. '2026-04-08'
    yesterday : str           e.g. '2026-04-07'

    Returns
    -------
    pd.DataFrame
        One row per sym with today/yesterday metrics and change columns:
            pct_change    - percentage change in avg spread
            abs_change    - absolute change in avg spread
            std_pct_change - percentage change in spread std deviation
            direction     - WIDENED / TIGHTENED / STABLE / VOLATILE
    """
    today_dt     = pd.Timestamp(today)
    yesterday_dt = pd.Timestamp(yesterday)

    today_df = (
        agg[agg['date'] == today_dt]
        .drop(columns='date')
        .rename(columns=lambda c: f"{c}_today" if c != 'sym' else c)
    )

    yesterday_df = (
        agg[agg['date'] == yesterday_dt]
        .drop(columns='date')
        .rename(columns=lambda c: f"{c}_yesterday" if c != 'sym' else c)
    )

    # Merge on sym — inner join so we only compare pairs present in both days
    merged = today_df.merge(yesterday_df, on='sym', how='inner')

    # ── Change metrics ────────────────────────────────────────────────────────
    merged['pct_change'] = (
        (merged['avg_spread_today'] - merged['avg_spread_yesterday'])
        / merged['avg_spread_yesterday']
        * 100
    ).round(2)

    merged['abs_change'] = (
        merged['avg_spread_today'] - merged['avg_spread_yesterday']
    )

    merged['std_pct_change'] = (
        (merged['std_spread_today'] - merged['std_spread_yesterday'])
        / merged['std_spread_yesterday']
        * 100
    ).round(2)

    # ── Direction classification ──────────────────────────────────────────────
    def classify(row):
        if row['std_pct_change'] > 20:
            return 'VOLATILE'
        elif row['pct_change'] > 5:
            return 'WIDENED'
        elif row['pct_change'] < -5:
            return 'TIGHTENED'
        else:
            return 'STABLE'

    merged['direction'] = merged.apply(classify, axis=1)

    return merged


if __name__ == '__main__':
    from mock_data  import generate_mock_data
    from cleaner    import clean_quotes
    from calculator import calculate_spreads
    from aggregator import aggregate_metrics

    today     = '2026-04-08'
    yesterday = '2026-04-07'

    df  = generate_mock_data(today, yesterday)
    df  = clean_quotes(df)
    df  = calculate_spreads(df)
    agg = aggregate_metrics(df)
    cmp = compare_days(agg, today, yesterday)

    print("Day-on-day comparison:")
    cols = ['sym', 'avg_spread_today', 'avg_spread_yesterday', 'pct_change', 'direction']
    print(cmp[cols].to_string(index=False))
