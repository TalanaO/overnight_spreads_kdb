import pandas as pd
import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
# Each pair maps to its typical mid price
PAIRS = {
    'EURUSD': 1.0800,
    'GBPUSD': 1.2650,
    'USDJPY': 151.50,
    'AUDUSD': 0.6550,
    'USDCAD': 1.3600,
}

# Typical spread scale per pair (in price terms, not pips)
SPREAD_SCALE = {
    'EURUSD': 0.00008,
    'GBPUSD': 0.00010,
    'USDJPY': 0.008,
    'AUDUSD': 0.00010,
    'USDCAD': 0.00012,
}

SESSIONS = [
    ('ASIA',   '00:00', '08:00'),
    ('LONDON', '08:00', '16:00'),
    ('NY',     '13:00', '21:00'),
]

VENUES = ['LP1', 'LP2', 'LP3', 'ECN1']


# ── Step 1: Timestamps ────────────────────────────────────────────────────────
def generate_timestamps(date: str, n: int) -> pd.Series:
    start = pd.Timestamp(date)
    end   = start + pd.Timedelta(hours=24)
    random_times = np.random.randint(start.value, end.value, n)
    random_times = pd.Series(pd.to_datetime(random_times))
    return random_times.sort_values().reset_index(drop=True)


# ── Step 2: Session label ─────────────────────────────────────────────────────
def assign_session(timestamps: pd.Series, date: str) -> pd.Series:
    """Label each timestamp with its trading session."""
    labels = pd.Series(['OFF'] * len(timestamps), index=timestamps.index)
    for name, start_str, end_str in SESSIONS:
        start = pd.Timestamp(f"{date} {start_str}")
        end   = pd.Timestamp(f"{date} {end_str}")
        mask  = (timestamps >= start) & (timestamps < end)
        labels[mask] = name
    return labels


# ── Step 3: Single pair data ──────────────────────────────────────────────────
def generate_pair_data(date: str, sym: str, base_mid: float, n: int = 750) -> pd.DataFrame:
    # Timestamps
    timestamps = generate_timestamps(date, n)

    # Mid price: random walk around base_mid
    steps = np.random.normal(loc=0, scale=base_mid * 0.0002, size=n)
    mid   = base_mid + np.cumsum(steps)

    # Spread: mostly small, occasionally spikes
    spread_scale = SPREAD_SCALE[sym]
    spread = np.abs(np.random.normal(loc=spread_scale, scale=spread_scale * 0.3, size=n))

    # Bid and ask around mid
    bid = mid - (spread / 2)
    ask = mid + (spread / 2)

    # Session labels and random venue
    session = assign_session(timestamps, date)
    venue   = np.random.choice(VENUES, size=n)

    df = pd.DataFrame({
        'timestamp': timestamps,
        'sym':       sym,
        'bid':       bid,
        'ask':       ask,
        'session':   session,
        'venue':     venue,
        'date':      pd.Timestamp(date).date(),
    })

    return df


# ── Step 4: Full dataset ──────────────────────────────────────────────────────
def generate_mock_data(today: str, yesterday: str, n: int = 750) -> pd.DataFrame:
    """
    Generate mock FX quote data for all pairs across two days.

    Parameters
    ----------
    today     : str  e.g. '2026-04-08'
    yesterday : str  e.g. '2026-04-07'
    n         : int  rows per pair per day

    Returns
    -------
    pd.DataFrame with all pairs and both days stacked
    """
    frames = []

    for date in [today, yesterday]:
        for sym, base_mid in PAIRS.items():
            df = generate_pair_data(date, sym, base_mid, n)
            frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    # Sort by timestamp so data looks like a real feed
    combined = combined.sort_values('timestamp').reset_index(drop=True)

    return combined


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    today     = '2026-04-08'
    yesterday = '2026-04-07'

    df = generate_mock_data(today, yesterday)

    print(f"Total rows : {len(df)}")
    print(f"Pairs      : {df['sym'].unique().tolist()}")
    print(f"Dates      : {df['date'].unique().tolist()}")
    print(f"Sessions   : {df['session'].unique().tolist()}")
    print(f"Venues     : {df['venue'].unique().tolist()}")
    print()
    print("Sample rows:")
    print(df.head(10).to_string(index=False))
    print()
    print("Spread stats per pair (today):")
    today_df = df[df['date'] == pd.Timestamp(today).date()].copy()
    today_df['spread'] = today_df['ask'] - today_df['bid']
    print(
        today_df.groupby('sym')['spread']
        .agg(avg='mean', median='median', max='max', std='std')
        .round(6)
        .to_string()
    )
