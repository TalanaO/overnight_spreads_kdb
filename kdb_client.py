import pandas as pd
from datetime import date, timedelta


# ── Query templates ───────────────────────────────────────────────────────────
QUOTE_QUERY = """
select date, time, sym, bid, ask
from quotes
where date in ({dates}),
      sym in ({syms})
"""


def get_quotes(today: str, yesterday: str, pairs: list,
               host: str = 'localhost', port: int = 5001) -> pd.DataFrame:
    """
    Retrieves FX spot quote data from kdb+ for two trading days.

    Requires PyKX: pip install pykx

    Parameters
    ----------
    today     : str   e.g. '2026-04-08'
    yesterday : str   e.g. '2026-04-07'
    pairs     : list  e.g. ['EURUSD', 'GBPUSD']
    host      : str   kdb+ hostname
    port      : int   kdb+ port

    Returns
    -------
    pd.DataFrame with columns: timestamp, sym, bid, ask
    """
    try:
        import pykx as kx
    except ImportError:
        raise ImportError(
            "PyKX is not installed. Run: pip install pykx\n"
            "Or use mock_data.generate_mock_data() for development."
        )

    # Format dates and syms for q query
    date_str = "; ".join([
        f"{pd.Timestamp(today).strftime('%Y.%m.%d')}",
        f"{pd.Timestamp(yesterday).strftime('%Y.%m.%d')}",
    ])
    sym_str = "`" + "`".join(pairs)

    query = QUOTE_QUERY.format(dates=date_str, syms=sym_str)

    try:
        conn = kx.SyncQConnection(host=host, port=port)
        df   = conn(query).pd()
        conn.close()
    except Exception as e:
        raise ConnectionError(f"kdb+ connection failed ({host}:{port}): {e}")

    # Normalise column names to match the rest of the pipeline
    df = df.rename(columns={'time': 'time_col'})
    df['timestamp'] = pd.to_datetime(df['date'].astype(str)) + pd.to_timedelta(df['time_col'])
    df = df.drop(columns=['time_col'])

    return df


def get_quotes_or_mock(today: str, yesterday: str, pairs: list,
                       host: str = 'localhost', port: int = 5001,
                       fallback_to_mock: bool = True) -> pd.DataFrame:
    """
    Tries kdb+ first. If unavailable and fallback_to_mock=True,
    returns synthetic data instead. Useful during development.

    Parameters
    ----------
    fallback_to_mock : bool
        If True, returns mock data when kdb+ is unreachable.
        Set to False in production.
    """
    try:
        df = get_quotes(today, yesterday, pairs, host, port)
        print(f"  Data source: kdb+ ({host}:{port})")
        return df
    except Exception as e:
        if fallback_to_mock:
            print(f"  kdb+ unavailable ({e})")
            print(f"  Falling back to mock data for development")
            from mock_data import generate_mock_data
            return generate_mock_data(today, yesterday)
        else:
            raise


if __name__ == '__main__':
    # In development this will fall back to mock data automatically
    today     = '2026-04-08'
    yesterday = '2026-04-07'
    pairs     = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCAD']

    df = get_quotes_or_mock(today, yesterday, pairs, fallback_to_mock=True)
    print(f"Rows returned: {len(df)}")
    print(df.head(5).to_string(index=False))
