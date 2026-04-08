import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText


def send_report(report: dict, config: dict) -> bool:
    """
    Renders the report dict as an HTML email and sends via SMTP.

    Parameters
    ----------
    report : dict   Output of reporter.build_report()
    config : dict   Email config with keys:
                    smtp_host, smtp_port, smtp_user, smtp_pass,
                    from_addr, to_addrs (list)

    Returns
    -------
    bool  True if sent successfully, False otherwise
    """
    html = _render_html(report)
    text = _render_text(report)

    msg = MIMEMultipart('alternative')
    msg['Subject'] = _subject(report)
    msg['From']    = config['from_addr']
    msg['To']      = ', '.join(config['to_addrs'])

    msg.attach(MIMEText(text, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    try:
        with smtplib.SMTP(config['smtp_host'], config['smtp_port']) as server:
            server.ehlo()
            server.starttls()
            server.login(config['smtp_user'], config['smtp_pass'])
            server.sendmail(config['from_addr'], config['to_addrs'], msg.as_string())
        print(f"  Email sent to: {config['to_addrs']}")
        return True
    except Exception as e:
        print(f"  Email failed: {e}")
        _save_fallback(html, report['overview']['run_date'])
        return False


def _subject(report: dict) -> str:
    status = report['overview']['status']
    date   = report['overview']['run_date']
    n_ano  = report['overview']['total_anomalies']
    return f"[{status}] FX Spread Monitor — {date} — {n_ano} anomalies"


def _render_html(report: dict) -> str:
    ov  = report['overview']
    cmp = report['pair_summary']
    top = report['top_movers']
    ano = report['anomalies']

    status_colour = {'NORMAL': '#2e7d32', 'REVIEW': '#e65100', 'ALERT': '#b71c1c'}
    colour = status_colour.get(ov['status'], '#333')

    # ── Pair summary table rows ───────────────────────────────────────────────
    pair_rows = ""
    for _, row in cmp.iterrows():
        arrow = "▲" if row['pct_change'] > 0 else "▼"
        clr   = "#b71c1c" if row['pct_change'] > 0 else "#2e7d32"
        pair_rows += f"""
        <tr>
          <td><b>{row['sym']}</b></td>
          <td>{row['avg_spread_today']:.6f}</td>
          <td>{row['avg_spread_yesterday']:.6f}</td>
          <td>{row['avg_spread_pips_today']:.3f}</td>
          <td style="color:{clr};font-weight:bold">{arrow} {row['pct_change']:+.1f}%</td>
          <td>{row['direction']}</td>
        </tr>"""

    # ── Anomaly table rows ────────────────────────────────────────────────────
    ano_rows = ""
    if len(ano) == 0:
        ano_rows = "<tr><td colspan='4'>No anomalies detected.</td></tr>"
    else:
        sev_colour = {'HIGH': '#b71c1c', 'MEDIUM': '#e65100', 'LOW': '#555'}
        for _, row in ano.iterrows():
            sc = sev_colour.get(row['severity'], '#333')
            ts = str(row['timestamp'])[:19] if row['timestamp'] else '—'
            ano_rows += f"""
            <tr>
              <td><b>{row['sym']}</b></td>
              <td style="color:{sc};font-weight:bold">{row['severity']}</td>
              <td>{row['rule']}</td>
              <td>{row['detail']}</td>
            </tr>"""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#222;max-width:800px;margin:auto;padding:20px">

    <h2 style="border-bottom:3px solid #1b3a6b;padding-bottom:8px;color:#1b3a6b">
      FX Spread Monitor — {ov['run_date']}
    </h2>

    <table style="margin-bottom:20px">
      <tr><td><b>Status</b></td>
          <td style="color:{colour};font-weight:bold;padding-left:12px">{ov['status']}</td></tr>
      <tr><td><b>Pairs monitored</b></td>
          <td style="padding-left:12px">{', '.join(ov['pairs_monitored'])}</td></tr>
      <tr><td><b>Total anomalies</b></td>
          <td style="padding-left:12px">{ov['total_anomalies']}
            ({ov['high_severity']} HIGH, {ov['medium_severity']} MEDIUM)</td></tr>
      <tr><td><b>Prior session</b></td>
          <td style="padding-left:12px">{ov['prior_date']}</td></tr>
    </table>

    <h3 style="color:#1b3a6b">Interpretation</h3>
    <p style="background:#f5f5f5;padding:12px;border-left:4px solid #2e6da4">
      {report['interpretation']}
    </p>

    <h3 style="color:#1b3a6b">Pair Summary</h3>
    <table border="1" cellpadding="6" cellspacing="0"
           style="border-collapse:collapse;width:100%;font-size:13px">
      <tr style="background:#1b3a6b;color:white">
        <th>Pair</th><th>Avg Spread Today</th><th>Avg Spread Yesterday</th>
        <th>Avg Pips</th><th>Change</th><th>Direction</th>
      </tr>
      {pair_rows}
    </table>

    <h3 style="color:#1b3a6b;margin-top:24px">Anomalies</h3>
    <table border="1" cellpadding="6" cellspacing="0"
           style="border-collapse:collapse;width:100%;font-size:13px">
      <tr style="background:#1b3a6b;color:white">
        <th>Pair</th><th>Severity</th><th>Rule</th><th>Detail</th>
      </tr>
      {ano_rows}
    </table>

    <p style="color:#888;font-size:11px;margin-top:32px;border-top:1px solid #ddd;padding-top:8px">
      Generated by FX Spread Monitor — {ov['run_date']}
    </p>
    </body></html>
    """
    return html


def _render_text(report: dict) -> str:
    ov  = report['overview']
    cmp = report['pair_summary']
    ano = report['anomalies']

    lines = [
        f"FX SPREAD MONITOR — {ov['run_date']}",
        f"Status: {ov['status']}",
        f"Anomalies: {ov['total_anomalies']} ({ov['high_severity']} HIGH, {ov['medium_severity']} MEDIUM)",
        "",
        "INTERPRETATION",
        report['interpretation'],
        "",
        "PAIR SUMMARY",
        f"{'Pair':<10} {'Avg Today':>12} {'Avg Yest':>12} {'Pips':>8} {'Chg%':>8} {'Direction':<12}",
        "-" * 65,
    ]
    for _, row in cmp.iterrows():
        lines.append(
            f"{row['sym']:<10} {row['avg_spread_today']:>12.6f} "
            f"{row['avg_spread_yesterday']:>12.6f} "
            f"{row['avg_spread_pips_today']:>8.3f} "
            f"{row['pct_change']:>+8.1f}% "
            f"{row['direction']:<12}"
        )

    lines += ["", "ANOMALIES", "-" * 65]
    if len(ano) == 0:
        lines.append("None detected.")
    else:
        for _, row in ano.iterrows():
            ts = str(row['timestamp'])[:19] if row['timestamp'] else '—'
            lines.append(f"[{row['severity']}] {row['sym']} — {row['rule']} — {row['detail']}")

    return "\n".join(lines)


def _save_fallback(html: str, date: str):
    """Write report to file if email fails."""
    path = f"fx_report_{date}.html"
    with open(path, 'w') as f:
        f.write(html)
    print(f"  Report saved locally: {path}")


def preview_report(report: dict):
    """Print the plain-text report to stdout — useful for development."""
    print(_render_text(report))


if __name__ == '__main__':
    from mock_data  import generate_mock_data
    from cleaner    import clean_quotes
    from calculator import calculate_spreads
    from aggregator import aggregate_metrics
    from comparator import compare_days
    from detector   import detect_anomalies
    from reporter   import build_report

    today     = '2026-04-08'
    yesterday = '2026-04-07'

    df  = generate_mock_data(today, yesterday)
    df  = clean_quotes(df)
    df  = calculate_spreads(df)
    agg = aggregate_metrics(df)
    cmp = compare_days(agg, today, yesterday)
    ano = detect_anomalies(df, cmp)
    rpt = build_report(cmp, ano, today, yesterday)

    # Print to terminal instead of sending email
    preview_report(rpt)
