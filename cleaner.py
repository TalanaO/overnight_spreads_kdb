import pandas as pd
import numpy as np


def clean_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates and cleans raw quote data before processing.

    Checks performed:
        - Required columns are present
        - No null values in critical fields
        - bid < ask (crossed quotes removed)
        - bid and ask are positive
        - Timestamps are valid datetimes

    Parameters
    ----------
    df : pd.DataFrame
        Raw quote DataFrame

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with invalid rows removed
    """
    df = df.copy()
    original_len = len(df)

    # ── Required columns ──────────────────────────────────────────────────────
    required = ['timestamp', 'sym', 'bid', 'ask']
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # ── Drop nulls in critical fields ─────────────────────────────────────────
    df = df.dropna(subset=required)

    # ── Ensure correct types ──────────────────────────────────────────────────
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['bid']       = pd.to_numeric(df['bid'], errors='coerce')
    df['ask']       = pd.to_numeric(df['ask'], errors='coerce')
    df = df.dropna(subset=['bid', 'ask'])

    # ── Remove crossed quotes (ask <= bid) ────────────────────────────────────
    crossed = df['ask'] <= df['bid']
    if crossed.sum() > 0:
        print(f"  Removed {crossed.sum()} crossed quotes (ask <= bid)")
    df = df[~crossed]

    # ── Remove negative or zero prices ───────────────────────────────────────
    bad_prices = (df['bid'] <= 0) | (df['ask'] <= 0)
    if bad_prices.sum() > 0:
        print(f"  Removed {bad_prices.sum()} rows with non-positive prices")
    df = df[~bad_prices]

    # ── Sort by timestamp ─────────────────────────────────────────────────────
    df = df.sort_values('timestamp').reset_index(drop=True)

    removed = original_len - len(df)
    if removed > 0:
        print(f"  Cleaner: removed {removed} rows ({original_len} -> {len(df)})")
    else:
        print(f"  Cleaner: all {len(df)} rows passed validation")

    return df


if __name__ == '__main__':
    from mock_data import generate_mock_data

    df = generate_mock_data('2026-04-08', '2026-04-07')

    # Inject some bad rows to test cleaning
    bad = pd.DataFrame([
        {'timestamp': '2026-04-08 09:00:00', 'sym': 'EURUSD', 'bid': 1.0810, 'ask': 1.0800},  # crossed
        {'timestamp': '2026-04-08 09:00:01', 'sym': 'GBPUSD', 'bid': -1.0,   'ask': 1.2650},  # negative
        {'timestamp': None,                  'sym': 'USDJPY', 'bid': 151.5,   'ask': 151.51},  # null ts
    ])
    df = pd.concat([df, bad], ignore_index=True)

    clean = clean_quotes(df)
    print(f"\nFinal shape: {clean.shape}")
