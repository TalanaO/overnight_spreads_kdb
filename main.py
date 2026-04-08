"""
FX Spread Monitor — Main Orchestrator
======================================
Runs the full pipeline:
  kdb+ (or mock) -> clean -> calculate -> aggregate ->
  bucket -> compare -> detect -> report -> email

Usage:
    python main.py                    # uses today and yesterday
    python main.py 2026-04-08         # specify today explicitly
"""

import sys
import logging
import pandas as pd
from datetime import date, timedelta

from kdb_client import get_quotes_or_mock
from cleaner    import clean_quotes
from calculator import calculate_spreads
from aggregator import aggregate_metrics
from bucketer   import bucket_all
from comparator import compare_days
from detector   import detect_anomalies
from reporter   import build_report
from emailer    import preview_report, send_report

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = '%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PAIRS = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCAD']

KDB_CONFIG = {
    'host': 'localhost',
    'port': 5001,
}

EMAIL_CONFIG = {
    'smtp_host':  'smtp.example.com',
    'smtp_port':  587,
    'smtp_user':  'monitor@desk.internal',
    'smtp_pass':  'your-password-here',
    'from_addr':  'monitor@desk.internal',
    'to_addrs':   ['desk@trading.com'],
}

# Set to False in production to require a live kdb+ connection
USE_MOCK_FALLBACK = False

# Set to True to send email, False to print to terminal
SEND_EMAIL = False


def run(today: str, yesterday: str):
    log.info(f"FX Spread Monitor starting — today={today}, yesterday={yesterday}")

    # ── Step 1: Data retrieval ────────────────────────────────────────────────
    log.info("Step 1/8 — Retrieving quotes")
    df = get_quotes_or_mock(
        today, yesterday, PAIRS,
        host             = KDB_CONFIG['host'],
        port             = KDB_CONFIG['port'],
        fallback_to_mock = USE_MOCK_FALLBACK,
    )
    log.info(f"  {len(df)} rows retrieved")

    # ── Step 2: Cleaning ──────────────────────────────────────────────────────
    log.info("Step 2/8 — Cleaning data")
    df = clean_quotes(df)

    # ── Step 3: Spread calculation ────────────────────────────────────────────
    log.info("Step 3/8 — Calculating spreads")
    df = calculate_spreads(df)

    # ── Step 4: Aggregation ───────────────────────────────────────────────────
    log.info("Step 4/8 — Aggregating metrics")
    agg = aggregate_metrics(df)

    # ── Step 5: Bucketing ─────────────────────────────────────────────────────
    log.info("Step 5/8 — Bucketing into time windows")
    buckets = bucket_all(df)
    for freq, bdf in buckets.items():
        log.info(f"  {freq}: {len(bdf)} buckets")

    # ── Step 6: Day-on-day comparison ─────────────────────────────────────────
    log.info("Step 6/8 — Comparing today vs yesterday")
    cmp = compare_days(agg, today, yesterday)
    log.info(f"  Directions: {cmp['direction'].value_counts().to_dict()}")

    # ── Step 7: Anomaly detection ─────────────────────────────────────────────
    log.info("Step 7/8 — Detecting anomalies")
    ano = detect_anomalies(df, cmp)
    log.info(f"  {len(ano)} anomalies detected "
             f"({len(ano[ano['severity']=='HIGH'])} HIGH, "
             f"{len(ano[ano['severity']=='MEDIUM'])} MEDIUM)")

    # ── Step 8: Report and deliver ────────────────────────────────────────────
    log.info("Step 8/8 — Building and delivering report")
    rpt = build_report(cmp, ano, today, yesterday)
    log.info(f"  Status: {rpt['overview']['status']}")

    if SEND_EMAIL:
        success = send_report(rpt, EMAIL_CONFIG)
        if not success:
            log.warning("Email delivery failed — report saved locally")
    else:
        preview_report(rpt)

    log.info("Pipeline complete")
    return rpt


if __name__ == '__main__':
    if len(sys.argv) > 1:
        today_str = sys.argv[1]
    else:
        today_str = str(date.today())

    yesterday_str = str(date.fromisoformat(today_str) - timedelta(days=1))

    run(today_str, yesterday_str)
