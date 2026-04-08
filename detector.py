import pandas as pd
import numpy as np


# ── Default thresholds (overridable via config) ───────────────────────────────
DEFAULT_THRESHOLDS = {
    'threshold_widening_pct': 20.0,   # Rule 1: day-on-day avg spread increase
    'spike_std_multiplier':    3.0,   # Rule 2: individual quote spike
    'sustained_minutes':      15,     # Rule 3: sustained widening window
    'sustained_pct':          15.0,   # Rule 3: how much wider to count as elevated
    'extreme_max_multiple':    2.0,   # Rule 4: max vs median ratio
}


def detect_anomalies(df: pd.DataFrame, comparison: pd.DataFrame,
                     thresholds: dict = None) -> pd.DataFrame:
    """
    Applies four rule-based anomaly detection checks.

    Rule 1 — Threshold widening  : avg spread up >20% day-on-day
    Rule 2 — Spike               : individual quote > mean + 3*std
    Rule 3 — Sustained widening  : elevated for 15+ consecutive minutes
    Rule 4 — Extreme maximum     : max spread > 2x median spread

    Parameters
    ----------
    df         : pd.DataFrame  Calculated quote data (output of calculator)
    comparison : pd.DataFrame  Output of comparator.compare_days()
    thresholds : dict          Optional override of DEFAULT_THRESHOLDS

    Returns
    -------
    pd.DataFrame
        One row per anomaly with columns:
        sym, rule, severity, timestamp, value, threshold, detail
    """
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    anomalies = []

    for sym in df['sym'].unique():
        sym_df = df[df['sym'] == sym].copy()

        # Today's quotes only
        today_date = sym_df['date'].max()
        today_df   = sym_df[sym_df['date'] == today_date]

        mean_spread   = today_df['spread'].mean()
        std_spread    = today_df['spread'].std()
        median_spread = today_df['spread'].median()
        max_spread    = today_df['spread'].max()

        # ── Rule 1: Threshold widening ────────────────────────────────────────
        cmp_row = comparison[comparison['sym'] == sym]
        if not cmp_row.empty:
            pct_change = cmp_row['pct_change'].values[0]
            if pct_change > t['threshold_widening_pct']:
                anomalies.append({
                    'sym':       sym,
                    'rule':      'THRESHOLD_WIDENING',
                    'severity':  'MEDIUM',
                    'timestamp': None,
                    'value':     round(pct_change, 2),
                    'threshold': t['threshold_widening_pct'],
                    'detail':    f"Avg spread up {pct_change:.1f}% vs yesterday",
                })

        # ── Rule 2: Spike detection ───────────────────────────────────────────
        spike_threshold = mean_spread + t['spike_std_multiplier'] * std_spread
        spikes = today_df[today_df['spread'] > spike_threshold]
        for _, row in spikes.iterrows():
            std_devs = (row['spread'] - mean_spread) / std_spread
            anomalies.append({
                'sym':       sym,
                'rule':      'SPIKE',
                'severity':  'HIGH',
                'timestamp': row['timestamp'],
                'value':     round(row['spread'], 8),
                'threshold': round(spike_threshold, 8),
                'detail':    f"{std_devs:.1f} std devs above mean",
            })

        # ── Rule 3: Sustained widening ────────────────────────────────────────
        sustained_threshold = median_spread * (1 + t['sustained_pct'] / 100)
        today_df_sorted = today_df.set_index('timestamp').sort_index()
        elevated = today_df_sorted['spread'] > sustained_threshold

        # Resample to 1-min buckets, flag bucket if majority of quotes elevated
        elevated_resampled = elevated.resample('1min').mean() > 0.5
        consecutive = 0
        start_time  = None

        for ts, is_elevated in elevated_resampled.items():
            if is_elevated:
                if consecutive == 0:
                    start_time = ts
                consecutive += 1
                if consecutive == t['sustained_minutes']:
                    anomalies.append({
                        'sym':       sym,
                        'rule':      'SUSTAINED_WIDENING',
                        'severity':  'HIGH',
                        'timestamp': start_time,
                        'value':     round(float(elevated_resampled[start_time:ts].index[-1].timestamp()), 0),
                        'threshold': t['sustained_minutes'],
                        'detail':    f"Spread elevated >{t['sustained_pct']}% above median for {consecutive}+ consecutive minutes",
                    })
            else:
                consecutive = 0
                start_time  = None

        # ── Rule 4: Extreme maximum ───────────────────────────────────────────
        extreme_threshold = median_spread * t['extreme_max_multiple']
        if max_spread > extreme_threshold:
            ratio = max_spread / median_spread
            anomalies.append({
                'sym':       sym,
                'rule':      'EXTREME_MAX',
                'severity':  'MEDIUM',
                'timestamp': today_df.loc[today_df['spread'].idxmax(), 'timestamp'],
                'value':     round(max_spread, 8),
                'threshold': round(extreme_threshold, 8),
                'detail':    f"Max spread is {ratio:.1f}x the daily median",
            })

    result = pd.DataFrame(anomalies, columns=[
        'sym', 'rule', 'severity', 'timestamp', 'value', 'threshold', 'detail'
    ])

    return result


if __name__ == '__main__':
    from mock_data  import generate_mock_data
    from cleaner    import clean_quotes
    from calculator import calculate_spreads
    from aggregator import aggregate_metrics
    from comparator import compare_days

    today     = '2026-04-08'
    yesterday = '2026-04-07'

    df  = generate_mock_data(today, yesterday)
    df  = clean_quotes(df)
    df  = calculate_spreads(df)
    agg = aggregate_metrics(df)
    cmp = compare_days(agg, today, yesterday)
    ano = detect_anomalies(df, cmp)

    print(f"Total anomalies detected: {len(ano)}")
    print()
    print("By rule:")
    print(ano.groupby(['rule', 'severity']).size().to_string())
    print()
    print("Sample anomalies:")
    print(ano[['sym', 'rule', 'severity', 'detail']].head(10).to_string(index=False))
