# overnight_spreads_kdb

A Python + kdb+/q pipeline that monitors intraday FX spot spreads, detects overnight changes, and delivers a structured end-of-day summary report.

Built to reflect real eFX desk tooling — useful for execution quality monitoring, liquidity analysis, and trading desk awareness.

---

## What It Does

1. Pulls FX spot quote data from kdb+ (or synthetic mock data in development)
2. Computes bid-ask spreads, mid prices, and pip-normalised spreads
3. Aggregates metrics per currency pair per day (avg, median, max, std)
4. Compares today's spread profile against yesterday's
5. Detects anomalies using four rule-based checks
6. Produces a plain-text report and optionally delivers it via email

---

## Project Structure

```
overnight_spreads_kdb/
│
├── main.py          # Orchestrator — run this
│
├── mock_data.py     # Synthetic FX quote generator (development only)
├── kdb_client.py    # kdb+ connection and query layer
│
├── cleaner.py       # Schema validation and bad row removal
├── calculator.py    # Spread, mid, relative spread, pip calculations
├── aggregator.py    # Per-pair per-day summary statistics
├── bucketer.py      # Time-bucket aggregation (1min / 5min / 15min)
├── comparator.py    # Day-on-day alignment and change metrics
├── detector.py      # Rule-based anomaly detection
├── reporter.py      # Report assembly
└── emailer.py       # SMTP delivery and plain-text rendering
```

---

## Quickstart

### 1. Install dependencies

```bash
pip install pandas numpy pyyaml
```

### 2. Run with mock data

```bash
python main.py
```

This uses synthetic data by default — no kdb+ connection required.

### 3. Run for a specific date

```bash
python main.py 2026-04-07
```

---

## Connecting to kdb+

### Install PyKX

```bash
pip install pykx
```

PyKX requires a valid kdb+ licence. If you have a licence file, set the environment variable before running:

```bash
export QLIC=/path/to/your/licence/folder   # macOS / Linux
set QLIC=C:\path\to\licence\folder         # Windows
```

### Configure the connection

Open `main.py` and update the kdb+ config block:

```python
KDB_CONFIG = {
    'host': 'localhost',   # your kdb+ host
    'port': 5000,          # your kdb+ port
}
```

### Switch off mock data fallback

In `main.py`, change:

```python
USE_MOCK_FALLBACK = True
```

to:

```python
USE_MOCK_FALLBACK = False
```

The system will now require a live kdb+ connection to run.

### Expected kdb+ table structure

The system queries a table called `quotes` with the following schema:

```q
quotes: ([] date:`date$(); time:`timestamp$(); sym:`symbol$(); bid:`float$(); ask:`float$())
```

Optional columns (used if present):

```q
source:`symbol$()    / liquidity provider or venue
session:`symbol$()   / ASIA, LONDON, NY
```

---

## Anomaly Detection Rules

| Rule | Condition | Severity |
|------|-----------|----------|
| THRESHOLD_WIDENING | Avg spread up >20% day-on-day | MEDIUM |
| SPIKE | Individual quote >3 std devs above mean | HIGH |
| SUSTAINED_WIDENING | Spread elevated >15% above median for 15+ consecutive minutes | HIGH |
| EXTREME_MAX | Max spread >2x the daily median | MEDIUM |

All thresholds are configurable in `detector.py` under `DEFAULT_THRESHOLDS`.

---

## Report Status Levels

| Status | Meaning |
|--------|---------|
| NORMAL | Quiet day, no material changes |
| REVIEW | Some widening or isolated anomalies worth checking |
| ALERT | Material deterioration, review before next session |

---

## Email Delivery

To enable email delivery, open `main.py` and update:

```python
EMAIL_CONFIG = {
    'smtp_host':  'smtp.example.com',
    'smtp_port':  587,
    'smtp_user':  'your-username',
    'smtp_pass':  'your-password',
    'from_addr':  'monitor@yourdomain.com',
    'to_addrs':   ['desk@yourdomain.com'],
}

SEND_EMAIL = True
```

When `SEND_EMAIL = False` (default), the report prints to the terminal instead.

---

## Currency Pairs Monitored

| Pair | Description |
|------|-------------|
| EURUSD | Euro / US Dollar |
| GBPUSD | British Pound / US Dollar |
| USDJPY | US Dollar / Japanese Yen |
| AUDUSD | Australian Dollar / US Dollar |
| USDCAD | US Dollar / Canadian Dollar |

To add or remove pairs, edit the `PAIRS` list in `main.py`.

---

## Tech Stack

- Python 3.12+
- pandas
- numpy
- PyKX (kdb+ connection, production only)
- smtplib (email delivery, standard library)

---

## Development vs Production

| Setting | Development | Production |
|---------|-------------|------------|
| `USE_MOCK_FALLBACK` | `True` | `False` |
| `SEND_EMAIL` | `False` | `True` |
| Data source | `mock_data.py` | kdb+ via `kdb_client.py` |

---

## Background

FX spot spread behaviour is a key indicator of liquidity conditions, market quality, and execution environment. This system automates the daily monitoring task of comparing current spread conditions against the prior session — surfacing deteriorations, anomalies, and pairs requiring attention before the next trading day begins.

Relevant to: eFX trading desks, execution monitoring teams, quant developers, market structure analysts.