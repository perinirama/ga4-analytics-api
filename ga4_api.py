from flask import Flask, request, jsonify
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    FilterExpression,
    Filter,
)
from google.oauth2 import service_account
import json
from datetime import datetime, timedelta
import anthropic
import os
import plotly.graph_objects as go
import plotly.io as pio
import base64
from urllib.parse import urlparse

app = Flask(__name__)


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


# ============================================================
# LEGACY ENDPOINT (kept for backwards compatibility)
# ============================================================

@app.route('/analyze', methods=['POST'])
def analyze_ga4():
    """
    Basic GA4 analysis endpoint (legacy).
    Kept for backwards compatibility.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400

        property_id = data.get('property_id')
        credentials_dict = data.get('credentials')
        urls = data.get('urls', [])
        days_back = data.get('days_back', 7)

        if not property_id:
            return jsonify({"error": "property_id is required"}), 400
        if not credentials_dict:
            return jsonify({"error": "credentials are required"}), 400

        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        client = BetaAnalyticsDataClient(credentials=credentials)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        request_params = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="pagePath"),
                Dimension(name="deviceCategory"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="engagedSessions"),
            ],
            date_ranges=[DateRange(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d")
            )],
            limit=100
        )

        response = client.run_report(request_params)

        results = []
        for row in response.rows:
            results.append({
                "pagePath": row.dimension_values[0].value,
                "deviceCategory": row.dimension_values[1].value,
                "sessions": int(row.metric_values[0].value),
                "totalUsers": int(row.metric_values[1].value),
                "bounceRate": float(row.metric_values[2].value),
                "averageSessionDuration": float(row.metric_values[3].value),
                "engagedSessions": int(row.metric_values[4].value),
            })

        if urls:
            url_paths = [urlparse(u).path or '/' for u in urls if u]
            if url_paths:
                filtered = [r for r in results if any(
                    r['pagePath'] == p or r['pagePath'].startswith(p) for p in url_paths
                )]
                results = filtered if filtered else results

        aggregated = aggregate_basic(results)

        return jsonify({
            "success": True,
            "property_id": property_id,
            "date_range": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d")
            },
            "total_pages": len(aggregated),
            "data": list(aggregated.values())
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }), 500


def aggregate_basic(results):
    """Basic aggregation for legacy endpoint"""
    aggregated = {}
    for row in results:
        path = row['pagePath']
        if path not in aggregated:
            aggregated[path] = {
                "pagePath": path,
                "sessions": 0, "totalUsers": 0,
                "bounceRate": 0, "averageSessionDuration": 0,
                "engagedSessions": 0, "devices": []
            }
        aggregated[path]["sessions"] += row["sessions"]
        aggregated[path]["totalUsers"] += row["totalUsers"]
        aggregated[path]["engagedSessions"] += row["engagedSessions"]
        aggregated[path]["devices"].append({
            "device": row["deviceCategory"],
            "sessions": row["sessions"]
        })

    for path, data in aggregated.items():
        total = data["sessions"]
        if total > 0:
            data["bounceRate"] = round(
                sum(r["bounceRate"] * r["sessions"] for r in results if r["pagePath"] == path) / total, 4
            )
            data["averageSessionDuration"] = round(
                sum(r["averageSessionDuration"] * r["sessions"] for r in results if r["pagePath"] == path) / total, 2
            )
    return aggregated


# ============================================================
# GA4 DATA COLLECTION FUNCTIONS
# ============================================================

def run_ga4_report(client, property_id, dimensions, metrics, start_date, end_date, limit=500):
    """
    Generic GA4 report runner. Returns list of dicts.
    """
    request_params = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )],
        limit=limit
    )
    response = client.run_report(request_params)

    rows = []
    for row in response.rows:
        entry = {}
        for i, d in enumerate(dimensions):
            entry[d] = row.dimension_values[i].value
        for i, m in enumerate(metrics):
            val = row.metric_values[i].value
            # Try to parse as int first, then float
            try:
                entry[m] = int(val)
            except ValueError:
                try:
                    entry[m] = float(val)
                except ValueError:
                    entry[m] = val
        rows.append(entry)
    return rows


def get_page_performance(client, property_id, start_date, end_date, urls=None):
    """
    Detailed page performance with device and traffic source breakdown.
    """
    dimensions = [
        "pagePath", "deviceCategory", "sessionSource",
        "sessionMedium", "sessionDefaultChannelGroup"
    ]
    metrics = [
        "sessions", "totalUsers", "newUsers",
        "bounceRate", "averageSessionDuration",
        "engagedSessions", "userEngagementDuration",
        "screenPageViewsPerSession", "engagementRate",
        "eventCount"
    ]

    rows = run_ga4_report(client, property_id, dimensions, metrics, start_date, end_date)

    # Filter by URL paths if provided
    if urls:
        url_paths = [urlparse(u).path or '/' for u in urls if u]
        if url_paths:
            filtered = [r for r in rows if any(
                r['pagePath'] == p or r['pagePath'].startswith(p) for p in url_paths
            )]
            rows = filtered if filtered else rows

    # Aggregate by page
    pages = {}
    for row in rows:
        path = row['pagePath']
        if path not in pages:
            pages[path] = {
                "pagePath": path,
                "sessions": 0, "totalUsers": 0, "newUsers": 0,
                "engagedSessions": 0, "eventCount": 0,
                "userEngagementDuration": 0,
                "devices": {}, "sources": {}, "channels": {},
                "_weighted_bounce": 0, "_weighted_duration": 0,
                "_weighted_pages_per_session": 0, "_weighted_engagement_rate": 0,
            }

        p = pages[path]
        s = row['sessions']
        p["sessions"] += s
        p["totalUsers"] += row['totalUsers']
        p["newUsers"] += row['newUsers']
        p["engagedSessions"] += row['engagedSessions']
        p["eventCount"] += row['eventCount']
        p["userEngagementDuration"] += row['userEngagementDuration']
        p["_weighted_bounce"] += row['bounceRate'] * s
        p["_weighted_duration"] += row['averageSessionDuration'] * s
        p["_weighted_pages_per_session"] += row['screenPageViewsPerSession'] * s
        p["_weighted_engagement_rate"] += row['engagementRate'] * s

        # Device breakdown
        dev = row['deviceCategory']
        p["devices"][dev] = p["devices"].get(dev, 0) + s

        # Source breakdown
        src = f"{row['sessionSource']} / {row['sessionMedium']}"
        p["sources"][src] = p["sources"].get(src, 0) + s

        # Channel breakdown
        ch = row['sessionDefaultChannelGroup']
        p["channels"][ch] = p["channels"].get(ch, 0) + s

    # Calculate weighted averages
    for path, p in pages.items():
        t = p["sessions"]
        if t > 0:
            p["bounceRate"] = round(p["_weighted_bounce"] / t, 4)
            p["averageSessionDuration"] = round(p["_weighted_duration"] / t, 2)
            p["pagesPerSession"] = round(p["_weighted_pages_per_session"] / t, 2)
            p["engagementRate"] = round(p["_weighted_engagement_rate"] / t, 4)
            p["avgEngagementDuration"] = round(p["userEngagementDuration"] / t, 2)
            p["returningUsers"] = p["totalUsers"] - p["newUsers"]
        # Clean up internal fields
        for key in list(p.keys()):
            if key.startswith("_"):
                del p[key]

    return list(pages.values())


def get_event_data(client, property_id, start_date, end_date):
    """Get top events and their frequency."""
    rows = run_ga4_report(
        client, property_id,
        dimensions=["eventName"],
        metrics=["eventCount", "totalUsers"],
        start_date=start_date, end_date=end_date,
        limit=50
    )
    # Filter out default GA4 events that aren't useful
    noise_events = {'session_start', 'first_visit', 'page_view', 'user_engagement', 'scroll'}
    return [r for r in rows if r['eventName'] not in noise_events]


def get_landing_pages(client, property_id, start_date, end_date):
    """Get landing page performance."""
    return run_ga4_report(
        client, property_id,
        dimensions=["landingPage", "sessionDefaultChannelGroup"],
        metrics=["sessions", "totalUsers", "bounceRate", "engagementRate", "averageSessionDuration"],
        start_date=start_date, end_date=end_date,
        limit=100
    )


def get_exit_pages(client, property_id, start_date, end_date):
    """Get exit page data."""
    rows = run_ga4_report(
        client, property_id,
        dimensions=["pagePath"],
        metrics=["sessions"],
        start_date=start_date, end_date=end_date,
        limit=50
    )
    return rows


def get_geographic_data(client, property_id, start_date, end_date):
    """Get geographic breakdown."""
    return run_ga4_report(
        client, property_id,
        dimensions=["country", "city"],
        metrics=["sessions", "totalUsers", "engagementRate"],
        start_date=start_date, end_date=end_date,
        limit=50
    )


def get_time_of_day(client, property_id, start_date, end_date):
    """Get traffic patterns by hour and day of week."""
    return run_ga4_report(
        client, property_id,
        dimensions=["hour", "dayOfWeek"],
        metrics=["sessions", "engagementRate"],
        start_date=start_date, end_date=end_date
    )


def get_user_acquisition(client, property_id, start_date, end_date):
    """Get acquisition channel performance."""
    return run_ga4_report(
        client, property_id,
        dimensions=["sessionDefaultChannelGroup", "sessionSource", "sessionMedium"],
        metrics=["newUsers", "sessions", "engagementRate", "bounceRate", "averageSessionDuration"],
        start_date=start_date, end_date=end_date,
        limit=50
    )


def get_site_totals(client, property_id, start_date, end_date):
    """Get overall site-level totals (no page dimension)."""
    rows = run_ga4_report(
        client, property_id,
        dimensions=[],
        metrics=[
            "sessions", "totalUsers", "newUsers",
            "bounceRate", "averageSessionDuration",
            "engagedSessions", "engagementRate",
            "screenPageViewsPerSession", "eventCount"
        ],
        start_date=start_date, end_date=end_date
    )
    return rows[0] if rows else {}


# ============================================================
# MULTI-PERIOD DATA COLLECTION
# ============================================================

def collect_all_data(client, property_id, urls=None):
    """
    Collect GA4 data across three comparison periods:
    - Last 7 days vs previous 7 days
    - Last 30 days vs previous 30 days
    - Last 7 days vs same 7 days last year
    """
    now = datetime.now()

    periods = {
        "last_7_days": {
            "current_start": now - timedelta(days=7),
            "current_end": now,
            "previous_start": now - timedelta(days=14),
            "previous_end": now - timedelta(days=7),
            "label": "Last 7 days"
        },
        "last_30_days": {
            "current_start": now - timedelta(days=30),
            "current_end": now,
            "previous_start": now - timedelta(days=60),
            "previous_end": now - timedelta(days=30),
            "label": "Last 30 days"
        },
        "year_over_year": {
            "current_start": now - timedelta(days=7),
            "current_end": now,
            "previous_start": now - timedelta(days=365 + 7),
            "previous_end": now - timedelta(days=365),
            "label": "Last 7 days vs same period last year"
        }
    }

    all_data = {}

    for period_key, period in periods.items():
        cs = period["current_start"]
        ce = period["current_end"]
        ps = period["previous_start"]
        pe = period["previous_end"]

        current_totals = get_site_totals(client, property_id, cs, ce)
        previous_totals = get_site_totals(client, property_id, ps, pe)

        # Calculate changes
        changes = {}
        for metric in current_totals:
            curr_val = current_totals.get(metric, 0)
            prev_val = previous_totals.get(metric, 0)
            if isinstance(curr_val, (int, float)) and isinstance(prev_val, (int, float)):
                if prev_val > 0:
                    pct_change = round(((curr_val - prev_val) / prev_val) * 100, 1)
                else:
                    pct_change = None
                changes[metric] = {
                    "current": curr_val,
                    "previous": prev_val,
                    "change_pct": pct_change
                }

        all_data[period_key] = {
            "label": period["label"],
            "date_range": {
                "current": f"{cs.strftime('%Y-%m-%d')} to {ce.strftime('%Y-%m-%d')}",
                "previous": f"{ps.strftime('%Y-%m-%d')} to {pe.strftime('%Y-%m-%d')}"
            },
            "totals": changes
        }

    # Detailed data for the primary period (last 7 days)
    cs7 = periods["last_7_days"]["current_start"]
    ce7 = periods["last_7_days"]["current_end"]
    ps7 = periods["last_7_days"]["previous_start"]
    pe7 = periods["last_7_days"]["previous_end"]

    all_data["page_performance"] = {
        "current": get_page_performance(client, property_id, cs7, ce7, urls),
        "previous": get_page_performance(client, property_id, ps7, pe7, urls)
    }
    all_data["events"] = get_event_data(client, property_id, cs7, ce7)
    all_data["landing_pages"] = get_landing_pages(client, property_id, cs7, ce7)
    all_data["exit_pages"] = get_exit_pages(client, property_id, cs7, ce7)
    all_data["geographic"] = get_geographic_data(client, property_id, cs7, ce7)
    all_data["time_of_day"] = get_time_of_day(client, property_id, cs7, ce7)
    all_data["acquisition"] = get_user_acquisition(client, property_id, cs7, ce7)

    # Also get 30-day page performance for comparison
    cs30 = periods["last_30_days"]["current_start"]
    ce30 = periods["last_30_days"]["current_end"]
    all_data["page_performance_30d"] = get_page_performance(client, property_id, cs30, ce30, urls)

    return all_data, periods


# ============================================================
# AI ANALYSIS WITH CLAUDE
# ============================================================

def build_data_summary(all_data, periods):
    """
    Build a structured text summary of all GA4 data for the AI prompt.
    """
    lines = []

    # --- Site-level trends across all periods ---
    lines.append("=" * 60)
    lines.append("SITE-LEVEL TRENDS ACROSS PERIODS")
    lines.append("=" * 60)

    for period_key in ["last_7_days", "last_30_days", "year_over_year"]:
        pd = all_data[period_key]
        lines.append(f"\n--- {pd['label']} ---")
        lines.append(f"Current period: {pd['date_range']['current']}")
        lines.append(f"Comparison period: {pd['date_range']['previous']}")

        totals = pd.get("totals", {})
        for metric, vals in totals.items():
            curr = vals['current']
            prev = vals['previous']
            pct = vals['change_pct']
            pct_str = f"{pct:+.1f}%" if pct is not None else "N/A"

            # Format rates as percentages
            if metric in ('bounceRate', 'engagementRate'):
                lines.append(f"  {metric}: {curr:.1%} (was {prev:.1%}, change: {pct_str})")
            elif metric == 'averageSessionDuration':
                lines.append(f"  {metric}: {curr:.1f}s (was {prev:.1f}s, change: {pct_str})")
            else:
                lines.append(f"  {metric}: {curr:,} (was {prev:,}, change: {pct_str})")

    # --- Page performance (current 7 days) ---
    lines.append("\n" + "=" * 60)
    lines.append("PAGE PERFORMANCE — LAST 7 DAYS")
    lines.append("=" * 60)

    current_pages = all_data.get("page_performance", {}).get("current", [])
    previous_pages = all_data.get("page_performance", {}).get("previous", [])
    prev_lookup = {p['pagePath']: p for p in previous_pages}

    # Sort by sessions descending
    current_pages_sorted = sorted(current_pages, key=lambda x: x['sessions'], reverse=True)

    for page in current_pages_sorted[:15]:  # Top 15 pages
        path = page['pagePath']
        lines.append(f"\n  Page: {path}")
        lines.append(f"    Sessions: {page['sessions']:,}")
        lines.append(f"    Users: {page['totalUsers']:,} (new: {page['newUsers']:,}, returning: {page.get('returningUsers', 0):,})")
        lines.append(f"    Bounce rate: {page['bounceRate']:.1%}")
        lines.append(f"    Engagement rate: {page['engagementRate']:.1%}")
        lines.append(f"    Avg session duration: {page['averageSessionDuration']:.1f}s")
        lines.append(f"    Avg engagement duration: {page.get('avgEngagementDuration', 0):.1f}s")
        lines.append(f"    Pages per session: {page.get('pagesPerSession', 0):.1f}")
        lines.append(f"    Events fired: {page['eventCount']:,}")

        # Device split
        devices = page.get('devices', {})
        if devices:
            dev_str = ", ".join(f"{k}: {v}" for k, v in sorted(devices.items(), key=lambda x: -x[1]))
            lines.append(f"    Devices: {dev_str}")

        # Top channels
        channels = page.get('channels', {})
        if channels:
            ch_str = ", ".join(f"{k}: {v}" for k, v in sorted(channels.items(), key=lambda x: -x[1])[:5])
            lines.append(f"    Top channels: {ch_str}")

        # Top sources
        sources = page.get('sources', {})
        if sources:
            src_str = ", ".join(f"{k}: {v}" for k, v in sorted(sources.items(), key=lambda x: -x[1])[:5])
            lines.append(f"    Top sources: {src_str}")

        # Week-over-week comparison
        prev = prev_lookup.get(path)
        if prev:
            s_change = page['sessions'] - prev['sessions']
            s_pct = ((s_change / prev['sessions']) * 100) if prev['sessions'] > 0 else 0
            lines.append(f"    vs previous 7 days: sessions {s_change:+,} ({s_pct:+.1f}%), "
                         f"bounce {page['bounceRate'] - prev['bounceRate']:+.1%}")

    # --- 30-day page performance ---
    lines.append("\n" + "=" * 60)
    lines.append("PAGE PERFORMANCE — LAST 30 DAYS")
    lines.append("=" * 60)

    pages_30d = all_data.get("page_performance_30d", [])
    pages_30d_sorted = sorted(pages_30d, key=lambda x: x['sessions'], reverse=True)

    for page in pages_30d_sorted[:10]:
        path = page['pagePath']
        lines.append(f"\n  Page: {path}")
        lines.append(f"    Sessions: {page['sessions']:,} | Users: {page['totalUsers']:,}")
        lines.append(f"    Bounce: {page['bounceRate']:.1%} | Engagement: {page['engagementRate']:.1%}")
        lines.append(f"    Avg duration: {page['averageSessionDuration']:.1f}s")

    # --- Events ---
    lines.append("\n" + "=" * 60)
    lines.append("USER EVENTS (excluding default GA4 events)")
    lines.append("=" * 60)

    events = all_data.get("events", [])
    events_sorted = sorted(events, key=lambda x: x.get('eventCount', 0), reverse=True)
    for ev in events_sorted[:15]:
        lines.append(f"  {ev['eventName']}: {ev['eventCount']:,} events by {ev['totalUsers']:,} users")

    # --- Landing pages ---
    lines.append("\n" + "=" * 60)
    lines.append("LANDING PAGES (entry points)")
    lines.append("=" * 60)

    landings = all_data.get("landing_pages", [])
    # Aggregate by landing page
    lp_agg = {}
    for lp in landings:
        path = lp['landingPage']
        if path not in lp_agg:
            lp_agg[path] = {"sessions": 0, "channels": {}}
        lp_agg[path]["sessions"] += lp['sessions']
        ch = lp['sessionDefaultChannelGroup']
        lp_agg[path]["channels"][ch] = lp_agg[path]["channels"].get(ch, 0) + lp['sessions']

    for path, info in sorted(lp_agg.items(), key=lambda x: -x[1]['sessions'])[:10]:
        ch_str = ", ".join(f"{k}: {v}" for k, v in sorted(info['channels'].items(), key=lambda x: -x[1])[:3])
        lines.append(f"  {path}: {info['sessions']:,} sessions (channels: {ch_str})")

    # --- Geographic ---
    lines.append("\n" + "=" * 60)
    lines.append("GEOGRAPHIC BREAKDOWN")
    lines.append("=" * 60)

    geo = all_data.get("geographic", [])
    # Aggregate by country
    country_agg = {}
    for g in geo:
        c = g['country']
        country_agg[c] = country_agg.get(c, 0) + g['sessions']
    for country, sess in sorted(country_agg.items(), key=lambda x: -x[1])[:10]:
        lines.append(f"  {country}: {sess:,} sessions")

    # Top cities
    geo_sorted = sorted(geo, key=lambda x: x['sessions'], reverse=True)
    lines.append("  Top cities:")
    for g in geo_sorted[:10]:
        lines.append(f"    {g['city']}, {g['country']}: {g['sessions']:,} sessions (engagement: {g['engagementRate']:.1%})")

    # --- Time of day ---
    lines.append("\n" + "=" * 60)
    lines.append("TRAFFIC PATTERNS BY TIME")
    lines.append("=" * 60)

    tod = all_data.get("time_of_day", [])
    # Aggregate by hour
    hour_agg = {}
    for t in tod:
        h = int(t['hour'])
        hour_agg[h] = hour_agg.get(h, 0) + t['sessions']
    if hour_agg:
        peak_hour = max(hour_agg, key=hour_agg.get)
        quiet_hour = min(hour_agg, key=hour_agg.get)
        lines.append(f"  Peak hour: {peak_hour}:00 ({hour_agg[peak_hour]:,} sessions)")
        lines.append(f"  Quietest hour: {quiet_hour}:00 ({hour_agg[quiet_hour]:,} sessions)")

    # Aggregate by day of week
    day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    day_agg = {}
    for t in tod:
        d = int(t['dayOfWeek'])
        day_agg[d] = day_agg.get(d, 0) + t['sessions']
    if day_agg:
        peak_day = max(day_agg, key=day_agg.get)
        lines.append(f"  Peak day: {day_names[peak_day]} ({day_agg[peak_day]:,} sessions)")
        for d in range(7):
            if d in day_agg:
                lines.append(f"    {day_names[d]}: {day_agg[d]:,}")

    # --- Acquisition ---
    lines.append("\n" + "=" * 60)
    lines.append("ACQUISITION CHANNELS")
    lines.append("=" * 60)

    acq = all_data.get("acquisition", [])
    # Aggregate by channel
    ch_agg = {}
    for a in acq:
        ch = a['sessionDefaultChannelGroup']
        if ch not in ch_agg:
            ch_agg[ch] = {"sessions": 0, "newUsers": 0, "_bounce_w": 0, "_eng_w": 0}
        ch_agg[ch]["sessions"] += a['sessions']
        ch_agg[ch]["newUsers"] += a['newUsers']
        ch_agg[ch]["_bounce_w"] += a['bounceRate'] * a['sessions']
        ch_agg[ch]["_eng_w"] += a['engagementRate'] * a['sessions']

    for ch, info in sorted(ch_agg.items(), key=lambda x: -x[1]['sessions']):
        s = info['sessions']
        br = info['_bounce_w'] / s if s > 0 else 0
        er = info['_eng_w'] / s if s > 0 else 0
        lines.append(f"  {ch}: {s:,} sessions, {info['newUsers']:,} new users, bounce: {br:.1%}, engagement: {er:.1%}")

    return "\n".join(lines)


def analyze_with_claude(all_data, periods, claude_api_key, urls=None, context=None):
    """
    Send comprehensive GA4 data to Claude for analysis.
    Returns the AI analysis as a string.
    """
    try:
        client = anthropic.Anthropic(api_key=claude_api_key)

        data_summary = build_data_summary(all_data, periods)

        # Build the prompt
        system_prompt = """You are a senior digital analytics consultant who produces clear, data-driven website performance reports. Your reports are valued because every insight is tied to a specific number from the data, and every recommendation explains exactly what to do and why it will work.

You write in British English. You never use filler phrases like "it's worth noting" or "interestingly". You are direct and specific.

IMPORTANT FORMATTING RULES:
- Your response must be valid HTML that renders well in an email client.
- Use inline CSS styles only (no <style> blocks, no external CSS).
- Use a clean, professional design with a white background.
- Use <h2> for main sections, <h3> for subsections.
- Use <table> with inline styles for data comparisons.
- Use <span style="color: #1a8754">▲</span> for positive changes and <span style="color: #dc3545">▼</span> for negative changes.
- Wrap key metrics in <strong> tags.
- Use <div> with light background colours for "plain English" summary boxes.
- Do NOT use markdown formatting. Only HTML.
- Do NOT include <html>, <head>, or <body> tags — just the content HTML."""

        user_prompt = f"""Analyse the following GA4 website analytics data and produce a comprehensive performance report.

{f'BUSINESS CONTEXT: {context}' if context else 'No specific business context provided — analyse from a general digital performance perspective.'}

{f'SPECIFIC URLs REQUESTED: {", ".join(urls)}' if urls else 'Analyse all pages in the data.'}

{data_summary}

REPORT STRUCTURE — follow this exactly:

1. EXECUTIVE SUMMARY
   - 3-4 sentence overview of overall website health
   - Include the single most important finding
   - State whether things are improving or declining and by how much

2. LEAD GENERATION & CONVERSION SIGNALS
   - Answer: "How are we tracking from a hot lead/joiner perspective?"
   - Look at conversion events, form submissions, contact events, booking events
   - Analyse engagement patterns that indicate purchase/enquiry intent (high engagement duration, multiple pages per session, specific event triggers)
   - If no explicit conversion events exist, identify proxy signals (engagement rate, session duration, pages per session, scroll events)
   - For each finding, add a "Plain English" box explaining what it means for the business

3. USER TRENDS & TRAFFIC HEALTH
   - Answer: "How is the number of users trending across the site?"
   - Compare 7-day, 30-day, and year-over-year trends
   - Break down new vs returning users and what that ratio means
   - Identify which acquisition channels are growing or shrinking
   - For each finding, add a "Plain English" box

4. BEHAVIOUR & ENGAGEMENT ANALYSIS
   - Answer: "How is website behaviour changing?"
   - Session duration trends — are people spending more or less time?
   - Bounce rate changes — are people leaving faster?
   - Pages per session — are people exploring more?
   - Device breakdown — how does mobile compare to desktop?
   - Time-of-day and day-of-week patterns — when are users most active and engaged?
   - Geographic patterns — where are the most engaged users?
   - For each finding, add a "Plain English" box

5. CONTENT PERFORMANCE
   - Answer: "What content is being best received?"
   - Compare pages against each other: which pages have the best engagement rate, lowest bounce rate, longest session duration?
   - Which landing pages are most effective at keeping users?
   - Which pages have the worst drop-off and why might that be?
   - If multiple pages exist, create an HTML comparison table with columns: Page, Sessions, Bounce Rate, Engagement Rate, Avg Duration, Verdict
   - For each finding, add a "Plain English" box

6. ACTIONABLE RECOMMENDATIONS
   Group into three priorities:
   a) DO THIS WEEK (quick wins, high impact, low effort)
   b) DO THIS MONTH (medium effort, significant impact)
   c) PLAN FOR NEXT QUARTER (strategic, requires resources)

   Each recommendation MUST:
   - Reference a specific metric from the data (e.g. "bounce rate on /pricing is 72%, which is 25pp above site average")
   - Explain exactly what to do (not "improve SEO" but "add a clear call-to-action above the fold on /services targeting the 340 weekly organic visitors who currently bounce at 65%")
   - Estimate the potential impact where possible

CRITICAL RULES:
- Every claim must reference a specific number from the data. No vague statements.
- If a metric looks anomalous (e.g. 8-second session duration with high engagement), flag it and explain possible causes.
- When comparing pages, always state which page is better and by how much.
- The "Plain English" boxes should be written as if explaining to a business owner who doesn't know analytics terminology. Use a <div> with style="background-color: #f0f7ff; border-left: 4px solid #4285F4; padding: 12px; margin: 10px 0; border-radius: 4px;" for these boxes.
- Format all percentages to 1 decimal place. Format all numbers with commas for thousands.
- If data seems insufficient or unusual, say so honestly rather than making up interpretations."""

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8000,
            messages=[
                {"role": "user", "content": user_prompt}
            ],
            system=system_prompt
        )

        return message.content[0].text

    except Exception as e:
        return f"<p style='color: red;'>Error generating AI insights: {str(e)}</p>"


# ============================================================
# CHART GENERATION
# ============================================================

def generate_charts(all_data):
    """
    Generate charts from GA4 data using Plotly.
    Returns dict of base64-encoded PNG images.
    """
    charts = {}

    try:
        # --- Chart 1: Sessions by page (top pages, last 7 days) ---
        current_pages = all_data.get("page_performance", {}).get("current", [])
        pages_sorted = sorted(current_pages, key=lambda x: x['sessions'], reverse=True)[:8]

        if pages_sorted:
            page_names = [p['pagePath'][:35] + ('...' if len(p['pagePath']) > 35 else '') for p in pages_sorted]
            sessions = [p['sessions'] for p in pages_sorted]

            fig1 = go.Figure(data=[
                go.Bar(y=page_names, x=sessions, orientation='h',
                       marker=dict(color='#4285F4'),
                       text=sessions, textposition='outside')
            ])
            fig1.update_layout(
                title='Sessions by page (last 7 days)',
                xaxis_title='Sessions',
                yaxis=dict(autorange="reversed"),
                height=max(300, len(pages_sorted) * 50 + 100),
                margin=dict(l=200, r=80, t=60, b=50),
                font=dict(size=12)
            )
            img = pio.to_image(fig1, format='png', width=800, height=max(300, len(pages_sorted) * 50 + 100))
            charts['sessions_chart'] = base64.b64encode(img).decode()

        # --- Chart 2: Bounce rate by page ---
        if pages_sorted:
            bounce_rates = [p['bounceRate'] * 100 for p in pages_sorted]
            colors = ['#EA4335' if br > 60 else '#FBBC04' if br > 40 else '#34A853' for br in bounce_rates]

            fig2 = go.Figure(data=[
                go.Bar(y=page_names, x=bounce_rates, orientation='h',
                       marker=dict(color=colors),
                       text=[f"{br:.1f}%" for br in bounce_rates], textposition='outside')
            ])
            fig2.update_layout(
                title='Bounce rate by page (%)',
                xaxis_title='Bounce Rate (%)',
                yaxis=dict(autorange="reversed"),
                height=max(300, len(pages_sorted) * 50 + 100),
                margin=dict(l=200, r=80, t=60, b=50),
                font=dict(size=12)
            )
            fig2.add_vline(x=50, line_dash="dash", line_color="gray", opacity=0.5)
            img = pio.to_image(fig2, format='png', width=800, height=max(300, len(pages_sorted) * 50 + 100))
            charts['bounce_rate_chart'] = base64.b64encode(img).decode()

        # --- Chart 3: Device breakdown (pie) ---
        device_totals = {}
        for page in current_pages:
            for dev, sess in page.get('devices', {}).items():
                device_totals[dev] = device_totals.get(dev, 0) + sess

        if device_totals:
            fig3 = go.Figure(data=[
                go.Pie(
                    labels=list(device_totals.keys()),
                    values=list(device_totals.values()),
                    marker=dict(colors=['#4285F4', '#EA4335', '#FBBC04', '#34A853']),
                    textinfo='label+percent',
                    textposition='inside'
                )
            ])
            fig3.update_layout(
                title='Traffic by device type',
                height=400,
                margin=dict(l=50, r=50, t=60, b=50),
                font=dict(size=13)
            )
            img = pio.to_image(fig3, format='png', width=600, height=400)
            charts['device_chart'] = base64.b64encode(img).decode()

        # --- Chart 4: Acquisition channels ---
        acq = all_data.get("acquisition", [])
        ch_agg = {}
        for a in acq:
            ch = a['sessionDefaultChannelGroup']
            ch_agg[ch] = ch_agg.get(ch, 0) + a['sessions']

        if ch_agg:
            ch_sorted = sorted(ch_agg.items(), key=lambda x: -x[1])[:8]
            ch_names = [c[0] for c in ch_sorted]
            ch_sessions = [c[1] for c in ch_sorted]

            fig4 = go.Figure(data=[
                go.Bar(x=ch_names, y=ch_sessions,
                       marker=dict(color='#34A853'),
                       text=ch_sessions, textposition='outside')
            ])
            fig4.update_layout(
                title='Sessions by acquisition channel',
                yaxis_title='Sessions',
                height=400,
                margin=dict(l=60, r=50, t=60, b=80),
                font=dict(size=12)
            )
            img = pio.to_image(fig4, format='png', width=800, height=400)
            charts['acquisition_chart'] = base64.b64encode(img).decode()

        # --- Chart 5: Engagement rate by page ---
        if pages_sorted:
            eng_rates = [p['engagementRate'] * 100 for p in pages_sorted]
            eng_colors = ['#34A853' if er > 60 else '#FBBC04' if er > 40 else '#EA4335' for er in eng_rates]

            fig5 = go.Figure(data=[
                go.Bar(y=page_names, x=eng_rates, orientation='h',
                       marker=dict(color=eng_colors),
                       text=[f"{er:.1f}%" for er in eng_rates], textposition='outside')
            ])
            fig5.update_layout(
                title='Engagement rate by page (%)',
                xaxis_title='Engagement Rate (%)',
                yaxis=dict(autorange="reversed"),
                height=max(300, len(pages_sorted) * 50 + 100),
                margin=dict(l=200, r=80, t=60, b=50),
                font=dict(size=12)
            )
            img = pio.to_image(fig5, format='png', width=800, height=max(300, len(pages_sorted) * 50 + 100))
            charts['engagement_chart'] = base64.b64encode(img).decode()

    except Exception as e:
        print(f"Error generating charts: {str(e)}")

    return charts


# ============================================================
# MAIN ENHANCED ENDPOINT
# ============================================================

@app.route('/analyze-with-ai', methods=['POST'])
def analyze_with_ai():
    """
    Enhanced endpoint with multi-period comparison and rich AI analysis.

    Expected JSON payload:
    {
        "property_id": "123456789",
        "credentials": { GA4 service account JSON },
        "urls": ["https://example.com/page1"],
        "days_back": 7,            // kept for compatibility but multi-period is automatic
        "claude_api_key": "sk-ant-...",
        "context": "Optional business context about this client"
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400

        claude_api_key = data.get('claude_api_key')
        if not claude_api_key:
            return jsonify({"error": "claude_api_key is required for AI analysis"}), 400

        property_id = data.get('property_id')
        credentials_dict = data.get('credentials')
        urls = data.get('urls', [])
        context = data.get('context', '')

        if not property_id or not credentials_dict:
            return jsonify({"error": "property_id and credentials are required"}), 400

        # Authenticate
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        ga4_client = BetaAnalyticsDataClient(credentials=credentials)

        # Collect all data across multiple periods
        all_data, periods = collect_all_data(ga4_client, property_id, urls)

        # Get AI analysis
        ai_insights = analyze_with_claude(all_data, periods, claude_api_key, urls, context)

        # Sanitise AI insights for JSON safety — remove control characters
        # that break JSON parsing in Make.com
        if isinstance(ai_insights, str):
            import re
            # Replace newlines and tabs with safe equivalents
            ai_insights = ai_insights.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            # Remove any remaining control characters (ASCII 0-31 except space)
            ai_insights = re.sub(r'[\x00-\x1f\x7f]', '', ai_insights)
            # Collapse multiple spaces into one
            ai_insights = re.sub(r' {2,}', ' ', ai_insights)

        # Generate charts
        charts = generate_charts(all_data)

        # Build response
        now = datetime.now()
        return jsonify({
            "success": True,
            "property_id": property_id,
            "date_range": {
                "start": (now - timedelta(days=7)).strftime("%Y-%m-%d"),
                "end": now.strftime("%Y-%m-%d")
            },
            "total_pages": len(all_data.get("page_performance", {}).get("current", [])),
            "ga4_data": all_data.get("page_performance", {}).get("current", []),
            "ai_insights": ai_insights,
            "charts": charts
        })

    except Exception as e:
        import traceback
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc()
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
