import pandas as pd
import numpy as np

# ── Pip factors ───────────────────────────────────────────────────────────────
# Multiply raw spread by this to get spread in pips
PIP_FACTORS = {
    'EURUSD': 10000,
    'GBPUSD': 10000,
    'AUDUSD': 10000,
    'USDCAD': 10000,
    'USDCHF': 10000,
    'NZDUSD': 10000,
    'USDJPY': 100,
}


def calculate_spreads(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes the raw quote DataFrame and adds derived spread columns.

    Input columns required:
        timestamp, sym, bid, ask, session, venue, date

    Output adds:
        spread           - raw price difference (ask - bid)
        mid              - midpoint price ((bid + ask) / 2)
        relative_spread  - spread as a fraction of mid (spread / mid)
        spread_pips      - spread converted to pips using PIP_FACTORS

    Parameters
    ----------
    df : pd.DataFrame
        Raw quote data from mock_data.py or kdb+

    Returns
    -------
    pd.DataFrame
        Original DataFrame with four new columns appended
    """
    df = df.copy()

    # ── Core calculations ─────────────────────────────────────────────────────
    df['spread'] = df['ask'] - df['bid']
    df['mid']    = (df['bid'] + df['ask']) / 2

    # Relative spread: what fraction of the mid price is the spread?
    # e.g. 0.000082 / 1.0800 = 0.0000759  (about 0.0076%)
    df['relative_spread'] = df['spread'] / df['mid']

    # Spread in pips: multiply by pip factor for the pair
    # EURUSD: 0.000082 * 10000 = 0.82 pips
    # USDJPY: 0.008    * 100   = 0.8  pips
    pip_map          = df['sym'].map(PIP_FACTORS)
    df['spread_pips'] = df['spread'] * pip_map

    # ── Sanity checks ─────────────────────────────────────────────────────────
    # These should never fire on clean data — useful during development
    negative_spreads = (df['spread'] < 0).sum()
    if negative_spreads > 0:
        print(f"  WARNING: {negative_spreads} rows have negative spreads (ask < bid) — check source data")

    zero_mid = (df['mid'] == 0).sum()
    if zero_mid > 0:
        print(f"  WARNING: {zero_mid} rows have zero mid price — relative_spread will be inf")

    unknown_pairs = df['sym'][pip_map.isna()].unique().tolist()
    if unknown_pairs:
        print(f"  WARNING: No pip factor defined for: {unknown_pairs} — spread_pips will be NaN")

    return df


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # Import the mock data generator to get test data
    from mock_data import generate_mock_data

    today     = '2026-04-08'
    yesterday = '2026-04-07'

    raw = generate_mock_data(today, yesterday)
    df  = calculate_spreads(raw)

    print(f"Columns now: {df.columns.tolist()}")
    print()
    print("Sample rows (key columns):")
    print(
        df[['sym', 'bid', 'ask', 'spread', 'mid', 'relative_spread', 'spread_pips']]
        .head(8)
        .to_string(index=False)
    )
    print()
    print("Spread in pips per pair (today, avg):")
    today_df = df[df['date'] == pd.Timestamp(today).date()]
    print(
        today_df.groupby('sym')['spread_pips']
        .mean()
        .round(3)
        .to_string()
    )
