import pandas as pd
from datetime import date


def build_report(comparison: pd.DataFrame, anomalies: pd.DataFrame,
                 today: str, yesterday: str) -> dict:
    """
    Assembles all pipeline outputs into a structured report dictionary.

    The report dict is consumed by emailer.py to render the final email.

    Sections:
        overview      - run metadata and overall status
        pair_summary  - full metrics table per pair
        top_movers    - top 3 pairs by absolute pct_change
        anomalies     - all flagged anomalies
        interpretation - auto-generated plain-English summary

    Parameters
    ----------
    comparison : pd.DataFrame  Output of comparator.compare_days()
    anomalies  : pd.DataFrame  Output of detector.detect_anomalies()
    today      : str           e.g. '2026-04-08'
    yesterday  : str           e.g. '2026-04-07'

    Returns
    -------
    dict
    """

    # ── Overall status ────────────────────────────────────────────────────────
    high_count   = len(anomalies[anomalies['severity'] == 'HIGH'])
    medium_count = len(anomalies[anomalies['severity'] == 'MEDIUM'])
    widened      = comparison[comparison['direction'] == 'WIDENED']

    if high_count >= 10 or len(widened) >= 3:
        status = 'ALERT'
    elif high_count >= 3 or medium_count >= 3 or len(widened) >= 1:
        status = 'REVIEW'
    else:
        status = 'NORMAL'

    # ── Overview section ──────────────────────────────────────────────────────
    overview = {
        'run_date':      today,
        'prior_date':    yesterday,
        'pairs_monitored': comparison['sym'].tolist(),
        'total_anomalies': len(anomalies),
        'high_severity':   high_count,
        'medium_severity': medium_count,
        'status':          status,
    }

    # ── Pair summary ──────────────────────────────────────────────────────────
    pair_summary = comparison[[
        'sym',
        'avg_spread_today', 'avg_spread_yesterday',
        'median_spread_today', 'median_spread_yesterday',
        'max_spread_today', 'max_spread_yesterday',
        'std_spread_today', 'std_spread_yesterday',
        'avg_spread_pips_today',
        'pct_change', 'abs_change', 'direction',
    ]].copy()

    pair_summary = pair_summary.round({
        'avg_spread_today':      8,
        'avg_spread_yesterday':  8,
        'pct_change':            2,
        'abs_change':            8,
        'avg_spread_pips_today': 3,
    })

    # ── Top movers ────────────────────────────────────────────────────────────
    top_movers = (
        comparison
        .assign(abs_pct=comparison['pct_change'].abs())
        .sort_values('abs_pct', ascending=False)
        .head(3)[['sym', 'pct_change', 'direction']]
    )

    # ── Interpretation ────────────────────────────────────────────────────────
    interpretation = _generate_interpretation(comparison, anomalies, status)

    return {
        'overview':       overview,
        'pair_summary':   pair_summary,
        'top_movers':     top_movers,
        'anomalies':      anomalies,
        'interpretation': interpretation,
    }


def _generate_interpretation(comparison, anomalies, status):
    """Auto-generates a plain-English paragraph summarising the day."""
    lines = []

    if status == 'NORMAL':
        lines.append("Today's spread environment was broadly stable.")
    elif status == 'REVIEW':
        lines.append("Today's spread environment showed some notable changes that warrant review.")
    else:
        lines.append("Today's spread environment deteriorated materially and requires attention.")

    widened   = comparison[comparison['direction'] == 'WIDENED']['sym'].tolist()
    tightened = comparison[comparison['direction'] == 'TIGHTENED']['sym'].tolist()
    volatile  = comparison[comparison['direction'] == 'VOLATILE']['sym'].tolist()

    if widened:
        lines.append(f"Spreads widened in: {', '.join(widened)}.")
    if tightened:
        lines.append(f"Spreads tightened in: {', '.join(tightened)}, suggesting improved liquidity.")
    if volatile:
        lines.append(f"Elevated intraday spread variability observed in: {', '.join(volatile)}.")

    high_anomalies = anomalies[anomalies['severity'] == 'HIGH']
    if not high_anomalies.empty:
        affected = high_anomalies['sym'].unique().tolist()
        lines.append(f"HIGH severity anomalies were flagged in: {', '.join(affected)}. Review before next session.")
    else:
        lines.append("No HIGH severity anomalies were detected.")

    return " ".join(lines)


if __name__ == '__main__':
    from mock_data  import generate_mock_data
    from cleaner    import clean_quotes
    from calculator import calculate_spreads
    from aggregator import aggregate_metrics
    from comparator import compare_days
    from detector   import detect_anomalies

    today     = '2026-04-08'
    yesterday = '2026-04-07'

    df  = generate_mock_data(today, yesterday)
    df  = clean_quotes(df)
    df  = calculate_spreads(df)
    agg = aggregate_metrics(df)
    cmp = compare_days(agg, today, yesterday)
    ano = detect_anomalies(df, cmp)
    rpt = build_report(cmp, ano, today, yesterday)

    print(f"Status  : {rpt['overview']['status']}")
    print(f"Anomalies: {rpt['overview']['total_anomalies']}")
    print()
    print("Interpretation:")
    print(rpt['interpretation'])
    print()
    print("Top movers:")
    print(rpt['top_movers'].to_string(index=False))
