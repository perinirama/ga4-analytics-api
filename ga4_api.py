from flask import Flask, request, jsonify
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    FilterExpression,
    FilterExpressionList,
    Filter,
)
from google.oauth2 import service_account
import json
import re
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
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


# ============================================================
# LEGACY ENDPOINT
# ============================================================

@app.route('/analyze', methods=['POST'])
def analyze_ga4():
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
            dimensions=[Dimension(name="pagePath"), Dimension(name="deviceCategory")],
            metrics=[
                Metric(name="sessions"), Metric(name="totalUsers"),
                Metric(name="bounceRate"), Metric(name="averageSessionDuration"),
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
            url_paths = [urlparse(u).path.rstrip('/') or '/' for u in urls if u]
            if url_paths:
                filtered = [r for r in results if r['pagePath'].rstrip('/') in url_paths]
                if filtered:
                    results = filtered

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
        return jsonify({"success": False, "error": str(e), "error_type": type(e).__name__}), 500


def aggregate_basic(results):
    aggregated = {}
    for row in results:
        path = row['pagePath']
        if path not in aggregated:
            aggregated[path] = {
                "pagePath": path, "sessions": 0, "totalUsers": 0,
                "bounceRate": 0, "averageSessionDuration": 0,
                "engagedSessions": 0, "devices": []
            }
        aggregated[path]["sessions"] += row["sessions"]
        aggregated[path]["totalUsers"] += row["totalUsers"]
        aggregated[path]["engagedSessions"] += row["engagedSessions"]
        aggregated[path]["devices"].append({"device": row["deviceCategory"], "sessions": row["sessions"]})

    for path, data in aggregated.items():
        total = data["sessions"]
        if total > 0:
            data["bounceRate"] = round(
                sum(r["bounceRate"] * r["sessions"] for r in results if r["pagePath"] == path) / total, 4)
            data["averageSessionDuration"] = round(
                sum(r["averageSessionDuration"] * r["sessions"] for r in results if r["pagePath"] == path) / total, 2)
    return aggregated


# ============================================================
# GA4 DATA COLLECTION FUNCTIONS
# ============================================================

def build_url_filter(url_paths, field_name="pagePath"):
    """
    Build a GA4 dimension filter that matches any of the given URL paths exactly.
    Uses IN_LIST filter when multiple paths, EXACT when single.
    """
    paths = list({p.rstrip('/') or '/' for p in url_paths})

    if len(paths) == 1:
        return FilterExpression(
            filter=Filter(
                field_name=field_name,
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.EXACT,
                    value=paths[0],
                    case_sensitive=False
                )
            )
        )
    else:
        return FilterExpression(
            or_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        filter=Filter(
                            field_name=field_name,
                            string_filter=Filter.StringFilter(
                                match_type=Filter.StringFilter.MatchType.EXACT,
                                value=p,
                                case_sensitive=False
                            )
                        )
                    )
                    for p in paths
                ]
            )
        )


def run_ga4_report(client, property_id, dimensions, metrics, start_date, end_date, limit=10000, url_filter=None):
    """
    Generic GA4 report runner. Returns list of dicts.
    Optionally applies a dimension filter at the API level.
    """
    kwargs = dict(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )],
        limit=limit
    )

    if url_filter is not None:
        kwargs['dimension_filter'] = url_filter

    request_params = RunReportRequest(**kwargs)
    response = client.run_report(request_params)

    rows = []
    for row in response.rows:
        entry = {}
        for i, d in enumerate(dimensions):
            entry[d] = row.dimension_values[i].value
        for i, m in enumerate(metrics):
            val = row.metric_values[i].value
            try:
                entry[m] = int(val)
            except ValueError:
                try:
                    entry[m] = float(val)
                except ValueError:
                    entry[m] = val
        rows.append(entry)
    return rows


def sanitise_url_list(raw_urls):
    if isinstance(raw_urls, str):
        raw_urls = re.split(r'[\n\r,]+', raw_urls)
    cleaned = []
    for u in raw_urls:
        u = re.sub(r'[\x00-\x1f\x7f]', '', u).strip()
        if u:
            cleaned.append(u)
    return cleaned


def get_url_paths(urls):
    """Convert a list of full URLs to normalised path strings."""
    paths = set()
    for u in urls:
        path = urlparse(u).path.rstrip('/') or '/'
        paths.add(path)
    return paths


def get_device_breakdown(client, property_id, start_date, end_date, urls=None):
    """
    Fetch sessions broken down by device category.
    Uses only 'sessions' as the metric — sessions are safely additive across
    device rows, unlike totalUsers which would be double-counted.
    Returns a dict: { pagePath: { device: sessions } }
    """
    url_filter = None
    if urls:
        url_paths = get_url_paths(urls)
        url_filter = build_url_filter(url_paths, field_name="pagePath")

    rows = run_ga4_report(
        client, property_id,
        dimensions=["pagePath", "deviceCategory"],
        metrics=["sessions"],
        start_date=start_date, end_date=end_date,
        limit=10000,
        url_filter=url_filter
    )

    devices_by_page = {}
    for row in rows:
        path = row['pagePath']
        if path not in devices_by_page:
            devices_by_page[path] = {}
        dev = row['deviceCategory']
        devices_by_page[path][dev] = devices_by_page[path].get(dev, 0) + row['sessions']

    return devices_by_page


def get_page_performance(client, property_id, start_date, end_date, urls=None):
    """
    Detailed page performance filtered at the GA4 API level.

    FIX: deviceCategory has been removed from the dimensions here.
    Previously, querying pagePath + deviceCategory + source/medium meant GA4
    returned one row per (page × device × source) combination. Summing
    totalUsers across those rows caused double/triple counting — a user visiting
    on desktop AND mobile was counted twice. Sessions are additive across devices
    but users are not.

    Device breakdown is now fetched separately via get_device_breakdown(), which
    only sums sessions (safe to aggregate) and never touches user counts.
    """
    print(f"[DEBUG] get_page_performance | {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} | urls={urls}", flush=True)

    dimensions = [
        "pagePath", "sessionSource",
        "sessionMedium", "sessionDefaultChannelGroup"
    ]
    metrics = [
        "sessions", "totalUsers", "newUsers",
        "bounceRate", "averageSessionDuration",
        "engagedSessions", "userEngagementDuration",
        "screenPageViewsPerSession", "engagementRate",
        "eventCount"
    ]

    url_filter = None
    url_paths = set()
    if urls:
        url_paths = get_url_paths(urls)
        url_filter = build_url_filter(url_paths, field_name="pagePath")
        print(f"[DEBUG] applying GA4 dimension filter for paths={url_paths}", flush=True)

    rows = run_ga4_report(
        client, property_id, dimensions, metrics,
        start_date, end_date,
        limit=10000,
        url_filter=url_filter
    )
    print(f"[DEBUG] GA4 returned {len(rows)} rows", flush=True)

    # Fetch device breakdown separately (sessions only — safe to sum)
    devices_by_page = get_device_breakdown(client, property_id, start_date, end_date, urls)

    # Aggregate by page across source/medium combinations
    pages = {}
    for row in rows:
        path = row['pagePath']
        if path not in pages:
            pages[path] = {
                "pagePath": path,
                "sessions": 0, "totalUsers": 0, "newUsers": 0,
                "engagedSessions": 0, "eventCount": 0,
                "userEngagementDuration": 0,
                "sources": {}, "channels": {},
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

        src = f"{row['sessionSource']} / {row['sessionMedium']}"
        p["sources"][src] = p["sources"].get(src, 0) + s

        ch = row['sessionDefaultChannelGroup']
        p["channels"][ch] = p["channels"].get(ch, 0) + s

    for path, p in pages.items():
        t = p["sessions"]
        if t > 0:
            p["bounceRate"] = round(p["_weighted_bounce"] / t, 4)
            p["averageSessionDuration"] = round(p["_weighted_duration"] / t, 2)
            p["pagesPerSession"] = round(p["_weighted_pages_per_session"] / t, 2)
            p["engagementRate"] = round(p["_weighted_engagement_rate"] / t, 4)
            p["avgEngagementDuration"] = round(p["userEngagementDuration"] / t, 2)
            p["returningUsers"] = p["totalUsers"] - p["newUsers"]
        # Attach device breakdown from the separate safe query
        p["devices"] = devices_by_page.get(path, {})
        for key in list(p.keys()):
            if key.startswith("_"):
                del p[key]

    result = list(pages.values())
    print(f"[DEBUG] get_page_performance result: {[(p['pagePath'], p['sessions'], p['totalUsers']) for p in result]}", flush=True)
    return result


def get_event_data(client, property_id, start_date, end_date, urls=None):
    url_filter = None
    if urls:
        url_paths = get_url_paths(urls)
        url_filter = build_url_filter(url_paths, field_name="pagePath")

    dimensions = ["eventName", "pagePath"] if urls else ["eventName"]
    rows = run_ga4_report(
        client, property_id,
        dimensions=dimensions,
        metrics=["eventCount", "totalUsers"],
        start_date=start_date, end_date=end_date,
        limit=500,
        url_filter=url_filter
    )
    noise_events = {'session_start', 'first_visit', 'page_view', 'user_engagement', 'scroll'}

    if urls:
        agg = {}
        for r in rows:
            name = r['eventName']
            if name not in agg:
                agg[name] = {'eventName': name, 'eventCount': 0, 'totalUsers': 0}
            agg[name]['eventCount'] += r['eventCount']
            agg[name]['totalUsers'] += r['totalUsers']
        rows = list(agg.values())

    return [r for r in rows if r['eventName'] not in noise_events]


def get_landing_pages(client, property_id, start_date, end_date, urls=None):
    url_filter = None
    if urls:
        url_paths = get_url_paths(urls)
        url_filter = build_url_filter(url_paths, field_name="landingPage")

    rows = run_ga4_report(
        client, property_id,
        dimensions=["landingPage", "sessionDefaultChannelGroup"],
        metrics=["sessions", "totalUsers", "bounceRate", "engagementRate", "averageSessionDuration"],
        start_date=start_date, end_date=end_date,
        limit=500,
        url_filter=url_filter
    )
    return rows


def get_exit_pages(client, property_id, start_date, end_date):
    return run_ga4_report(
        client, property_id,
        dimensions=["pagePath"],
        metrics=["sessions"],
        start_date=start_date, end_date=end_date,
        limit=50
    )


def get_geographic_data(client, property_id, start_date, end_date):
    return run_ga4_report(
        client, property_id,
        dimensions=["country", "city"],
        metrics=["sessions", "totalUsers", "engagementRate"],
        start_date=start_date, end_date=end_date,
        limit=50
    )


def get_time_of_day(client, property_id, start_date, end_date):
    return run_ga4_report(
        client, property_id,
        dimensions=["hour", "dayOfWeek"],
        metrics=["sessions", "engagementRate"],
        start_date=start_date, end_date=end_date
    )


def get_user_acquisition(client, property_id, start_date, end_date, urls=None):
    url_filter = None
    if urls:
        url_paths = get_url_paths(urls)
        url_filter = build_url_filter(url_paths, field_name="pagePath")

    dimensions = ["sessionDefaultChannelGroup", "sessionSource", "sessionMedium", "pagePath"] if urls else \
                 ["sessionDefaultChannelGroup", "sessionSource", "sessionMedium"]
    rows = run_ga4_report(
        client, property_id,
        dimensions=dimensions,
        metrics=["newUsers", "sessions", "engagementRate", "bounceRate", "averageSessionDuration"],
        start_date=start_date, end_date=end_date,
        limit=500,
        url_filter=url_filter
    )
    if urls:
        agg = {}
        for r in rows:
            key = (r['sessionDefaultChannelGroup'], r['sessionSource'], r['sessionMedium'])
            if key not in agg:
                agg[key] = {
                    'sessionDefaultChannelGroup': r['sessionDefaultChannelGroup'],
                    'sessionSource': r['sessionSource'],
                    'sessionMedium': r['sessionMedium'],
                    'newUsers': 0, 'sessions': 0,
                    '_eng_w': 0, '_bounce_w': 0, '_dur_w': 0
                }
            s = r['sessions']
            agg[key]['newUsers'] += r['newUsers']
            agg[key]['sessions'] += s
            agg[key]['_eng_w'] += r['engagementRate'] * s
            agg[key]['_bounce_w'] += r['bounceRate'] * s
            agg[key]['_dur_w'] += r['averageSessionDuration'] * s
        result = []
        for v in agg.values():
            s = v['sessions']
            v['engagementRate'] = round(v['_eng_w'] / s, 4) if s > 0 else 0
            v['bounceRate'] = round(v['_bounce_w'] / s, 4) if s > 0 else 0
            v['averageSessionDuration'] = round(v['_dur_w'] / s, 2) if s > 0 else 0
            del v['_eng_w'], v['_bounce_w'], v['_dur_w']
            result.append(v)
        return result
    return rows


def get_site_totals(client, property_id, start_date, end_date):
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


def aggregate_pages_to_totals(pages):
    if not pages:
        return {}

    total_sessions = sum(p['sessions'] for p in pages)
    total_users = sum(p['totalUsers'] for p in pages)
    total_new_users = sum(p['newUsers'] for p in pages)
    total_engaged = sum(p['engagedSessions'] for p in pages)
    total_events = sum(p['eventCount'] for p in pages)

    if total_sessions > 0:
        avg_bounce = sum(p['bounceRate'] * p['sessions'] for p in pages) / total_sessions
        avg_duration = sum(p['averageSessionDuration'] * p['sessions'] for p in pages) / total_sessions
        avg_engagement_rate = sum(p['engagementRate'] * p['sessions'] for p in pages) / total_sessions
        avg_pages_per_session = sum(p.get('pagesPerSession', 0) * p['sessions'] for p in pages) / total_sessions
    else:
        avg_bounce = avg_duration = avg_engagement_rate = avg_pages_per_session = 0

    return {
        'sessions': total_sessions,
        'totalUsers': total_users,
        'newUsers': total_new_users,
        'bounceRate': round(avg_bounce, 4),
        'averageSessionDuration': round(avg_duration, 2),
        'engagedSessions': total_engaged,
        'engagementRate': round(avg_engagement_rate, 4),
        'screenPageViewsPerSession': round(avg_pages_per_session, 2),
        'eventCount': total_events,
    }


# ============================================================
# MULTI-PERIOD DATA COLLECTION
# ============================================================

def collect_all_data(client, property_id, urls=None):
    now = datetime.now()
    print(f"[DEBUG] collect_all_data | property_id={property_id} | urls={urls} | now={now.strftime('%Y-%m-%d %H:%M')}", flush=True)

    periods = {
        "last_7_days": {
            "current_start": now - timedelta(days=7),
            "current_end": now,
            "label": f"Last 7 days ({(now - timedelta(days=7)).strftime('%d %b')} – {now.strftime('%d %b %Y')})"
        },
        "last_28_days": {
            "current_start": now - timedelta(days=28),
            "current_end": now,
            "previous_start": now - timedelta(days=56),
            "previous_end": now - timedelta(days=28),
            "label": "Last 28 days vs previous 28 days"
        },
        "year_over_year": {
            "current_start": now - timedelta(days=28),
            "current_end": now,
            "previous_start": now - timedelta(days=365 + 28),
            "previous_end": now - timedelta(days=365),
            "label": "Last 28 days vs same period last year"
        },
    }

    all_data = {}

    for period_key in ["last_28_days", "year_over_year"]:
        period = periods[period_key]
        cs = period["current_start"]
        ce = period["current_end"]
        ps = period["previous_start"]
        pe = period["previous_end"]

        print(f"[DEBUG] {period_key} | current={cs.strftime('%Y-%m-%d')} to {ce.strftime('%Y-%m-%d')} | previous={ps.strftime('%Y-%m-%d')} to {pe.strftime('%Y-%m-%d')}", flush=True)

        if urls:
            current_pages = get_page_performance(client, property_id, cs, ce, urls)
            previous_pages = get_page_performance(client, property_id, ps, pe, urls)
            current_totals = aggregate_pages_to_totals(current_pages)
            previous_totals = aggregate_pages_to_totals(previous_pages)
        else:
            current_totals = get_site_totals(client, property_id, cs, ce)
            previous_totals = get_site_totals(client, property_id, ps, pe)

        print(f"[DEBUG] {period_key} current_totals sessions={current_totals.get('sessions', 'N/A')} totalUsers={current_totals.get('totalUsers', 'N/A')} | previous_totals sessions={previous_totals.get('sessions', 'N/A')}", flush=True)

        changes = {}
        for metric in current_totals:
            curr_val = current_totals.get(metric, 0)
            prev_val = previous_totals.get(metric, 0)
            if isinstance(curr_val, (int, float)) and isinstance(prev_val, (int, float)):
                pct_change = round(((curr_val - prev_val) / prev_val) * 100, 1) if prev_val > 0 else None
                changes[metric] = {"current": curr_val, "previous": prev_val, "change_pct": pct_change}

        all_data[period_key] = {
            "label": period["label"],
            "date_range": {
                "current": f"{cs.strftime('%Y-%m-%d')} to {ce.strftime('%Y-%m-%d')}",
                "previous": f"{ps.strftime('%Y-%m-%d')} to {pe.strftime('%Y-%m-%d')}"
            },
            "totals": changes
        }

    cs7 = periods["last_7_days"]["current_start"]
    ce7 = periods["last_7_days"]["current_end"]
    print(f"[DEBUG] last_7_days | {cs7.strftime('%Y-%m-%d')} to {ce7.strftime('%Y-%m-%d')}", flush=True)
    all_data["last_7_days"] = {
        "label": periods["last_7_days"]["label"],
        "page_performance": get_page_performance(client, property_id, cs7, ce7, urls),
        "devices": get_user_acquisition(client, property_id, cs7, ce7, urls),
        "geographic": get_geographic_data(client, property_id, cs7, ce7),
    }

    cs28 = periods["last_28_days"]["current_start"]
    ce28 = periods["last_28_days"]["current_end"]
    ps28 = periods["last_28_days"]["previous_start"]
    pe28 = periods["last_28_days"]["previous_end"]

    all_data["page_performance"] = {
        "current": get_page_performance(client, property_id, cs28, ce28, urls),
        "previous": get_page_performance(client, property_id, ps28, pe28, urls)
    }
    all_data["events"] = get_event_data(client, property_id, cs28, ce28, urls)
    all_data["landing_pages"] = get_landing_pages(client, property_id, cs28, ce28, urls)
    all_data["exit_pages"] = get_exit_pages(client, property_id, cs28, ce28)
    all_data["geographic"] = get_geographic_data(client, property_id, cs28, ce28)
    all_data["time_of_day"] = get_time_of_day(client, property_id, cs28, ce28)
    all_data["acquisition"] = get_user_acquisition(client, property_id, cs28, ce28, urls)

    cs_yoy = periods["year_over_year"]["previous_start"]
    ce_yoy = periods["year_over_year"]["previous_end"]
    all_data["page_performance_yoy"] = get_page_performance(client, property_id, cs_yoy, ce_yoy, urls)

    print(f"[DEBUG] collect_all_data complete", flush=True)
    return all_data, periods


# ============================================================
# AI ANALYSIS WITH CLAUDE
# ============================================================

def build_data_summary(all_data, periods, urls=None):
    lines = []

    lines.append("=" * 60)
    lines.append(f"BLOCK 1 — {all_data.get('last_7_days', {}).get('label', 'Last 7 days')} — STANDALONE SNAPSHOT (no comparison)")
    lines.append("=" * 60)

    pages_7d = all_data.get("last_7_days", {}).get("page_performance", [])
    pages_7d_sorted = sorted(pages_7d, key=lambda x: x['sessions'], reverse=True)
    page_limit_7d = len(pages_7d_sorted) if urls else 20

    for page in pages_7d_sorted[:page_limit_7d]:
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
        devices = page.get('devices', {})
        if devices:
            dev_str = ", ".join(f"{k}: {v}" for k, v in sorted(devices.items(), key=lambda x: -x[1]))
            lines.append(f"    Devices: {dev_str}")
        channels = page.get('channels', {})
        if channels:
            ch_str = ", ".join(f"{k}: {v}" for k, v in sorted(channels.items(), key=lambda x: -x[1])[:5])
            lines.append(f"    Top channels: {ch_str}")

    geo_7d = all_data.get("last_7_days", {}).get("geographic", [])
    if geo_7d:
        lines.append("\n  Top cities (last 7 days):")
        for g in sorted(geo_7d, key=lambda x: x['sessions'], reverse=True)[:5]:
            lines.append(f"    {g['city']}, {g['country']}: {g['sessions']:,} sessions (engagement: {g['engagementRate']:.1%})")

    lines.append("\n" + "=" * 60)
    lines.append("BLOCK 2 — LAST 28 DAYS vs PREVIOUS 28 DAYS (same period last month)")
    lines.append("=" * 60)

    pd28 = all_data.get("last_28_days", {})
    lines.append(f"Current period: {pd28.get('date_range', {}).get('current', '')}")
    lines.append(f"Comparison period: {pd28.get('date_range', {}).get('previous', '')}")

    totals28 = pd28.get("totals", {})
    for metric, vals in totals28.items():
        curr = vals['current']
        prev = vals['previous']
        pct = vals['change_pct']
        pct_str = f"{pct:+.1f}%" if pct is not None else "N/A"
        if metric in ('bounceRate', 'engagementRate'):
            lines.append(f"  {metric}: {curr:.1%} (was {prev:.1%}, change: {pct_str})")
        elif metric == 'averageSessionDuration':
            lines.append(f"  {metric}: {curr:.1f}s (was {prev:.1f}s, change: {pct_str})")
        else:
            lines.append(f"  {metric}: {curr:,} (was {prev:,}, change: {pct_str})")

    lines.append("\n  Per-page detail (last 28 days vs previous 28 days):")
    current_pages = all_data.get("page_performance", {}).get("current", [])
    previous_pages = all_data.get("page_performance", {}).get("previous", [])
    prev_lookup = {p['pagePath']: p for p in previous_pages}
    current_pages_sorted = sorted(current_pages, key=lambda x: x['sessions'], reverse=True)
    page_limit = len(current_pages_sorted) if urls else 30

    for page in current_pages_sorted[:page_limit]:
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
        devices = page.get('devices', {})
        if devices:
            dev_str = ", ".join(f"{k}: {v}" for k, v in sorted(devices.items(), key=lambda x: -x[1]))
            lines.append(f"    Devices: {dev_str}")
        channels = page.get('channels', {})
        if channels:
            ch_str = ", ".join(f"{k}: {v}" for k, v in sorted(channels.items(), key=lambda x: -x[1])[:5])
            lines.append(f"    Top channels: {ch_str}")
        prev = prev_lookup.get(path)
        if prev:
            s_change = page['sessions'] - prev['sessions']
            s_pct = ((s_change / prev['sessions']) * 100) if prev['sessions'] > 0 else 0
            lines.append(f"    vs previous 28 days: sessions {s_change:+,} ({s_pct:+.1f}%), "
                         f"bounce {page['bounceRate'] - prev['bounceRate']:+.1%}")

    lines.append("\n" + "=" * 60)
    lines.append("BLOCK 3 — LAST 28 DAYS vs SAME PERIOD LAST YEAR (year-on-year)")
    lines.append("=" * 60)

    pdyoy = all_data.get("year_over_year", {})
    lines.append(f"Current period: {pdyoy.get('date_range', {}).get('current', '')}")
    lines.append(f"Comparison period (last year): {pdyoy.get('date_range', {}).get('previous', '')}")

    totals_yoy = pdyoy.get("totals", {})
    for metric, vals in totals_yoy.items():
        curr = vals['current']
        prev = vals['previous']
        pct = vals['change_pct']
        pct_str = f"{pct:+.1f}%" if pct is not None else "N/A"
        if metric in ('bounceRate', 'engagementRate'):
            lines.append(f"  {metric}: {curr:.1%} (was {prev:.1%}, change: {pct_str})")
        elif metric == 'averageSessionDuration':
            lines.append(f"  {metric}: {curr:.1f}s (was {prev:.1f}s, change: {pct_str})")
        else:
            lines.append(f"  {metric}: {curr:,} (was {prev:,}, change: {pct_str})")

    lines.append("\n  Per-page detail (last 28 days vs same period last year):")
    pages_yoy = all_data.get("page_performance_yoy", [])
    pages_yoy_sorted = sorted(pages_yoy, key=lambda x: x['sessions'], reverse=True)
    yoy_limit = len(pages_yoy_sorted) if urls else 30

    for page in pages_yoy_sorted[:yoy_limit]:
        path = page['pagePath']
        lines.append(f"\n  Page (last year): {path}")
        lines.append(f"    Sessions: {page['sessions']:,} | Users: {page['totalUsers']:,}")
        lines.append(f"    Bounce: {page['bounceRate']:.1%} | Engagement: {page['engagementRate']:.1%}")
        lines.append(f"    Avg duration: {page['averageSessionDuration']:.1f}s")

    lines.append("\n" + "=" * 60)
    lines.append("USER EVENTS (last 28 days, excluding default GA4 events)")
    lines.append("=" * 60)

    events = all_data.get("events", [])
    events_sorted = sorted(events, key=lambda x: x.get('eventCount', 0), reverse=True)
    for ev in events_sorted[:15]:
        lines.append(f"  {ev['eventName']}: {ev['eventCount']:,} events by {ev['totalUsers']:,} users")

    lines.append("\n" + "=" * 60)
    lines.append("LANDING PAGES (last 28 days)")
    lines.append("=" * 60)

    landings = all_data.get("landing_pages", [])
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

    lines.append("\n" + "=" * 60)
    lines.append("GEOGRAPHIC BREAKDOWN (last 28 days)")
    lines.append("=" * 60)

    geo = all_data.get("geographic", [])
    country_agg = {}
    for g in geo:
        c = g['country']
        country_agg[c] = country_agg.get(c, 0) + g['sessions']
    for country, sess in sorted(country_agg.items(), key=lambda x: -x[1])[:10]:
        lines.append(f"  {country}: {sess:,} sessions")
    geo_sorted = sorted(geo, key=lambda x: x['sessions'], reverse=True)
    lines.append("  Top cities:")
    for g in geo_sorted[:10]:
        lines.append(f"    {g['city']}, {g['country']}: {g['sessions']:,} sessions (engagement: {g['engagementRate']:.1%})")

    lines.append("\n" + "=" * 60)
    lines.append("TRAFFIC PATTERNS BY TIME (last 28 days)")
    lines.append("=" * 60)

    tod = all_data.get("time_of_day", [])
    hour_agg = {}
    for t in tod:
        h = int(t['hour'])
        hour_agg[h] = hour_agg.get(h, 0) + t['sessions']
    if hour_agg:
        peak_hour = max(hour_agg, key=hour_agg.get)
        quiet_hour = min(hour_agg, key=hour_agg.get)
        lines.append(f"  Peak hour: {peak_hour}:00 ({hour_agg[peak_hour]:,} sessions)")
        lines.append(f"  Quietest hour: {quiet_hour}:00 ({hour_agg[quiet_hour]:,} sessions)")

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

    lines.append("\n" + "=" * 60)
    lines.append("ACQUISITION CHANNELS (last 28 days)")
    lines.append("=" * 60)

    acq = all_data.get("acquisition", [])
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
    try:
        client = anthropic.Anthropic(api_key=claude_api_key)
        data_summary = build_data_summary(all_data, periods, urls)

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

        is_location_report = urls and len(urls) > 3 and all('find-a-gym' in u or 'location' in u or 'club' in u for u in urls[:3])
        location_context = """
IMPORTANT: The URLs being analysed are individual gym/venue location pages. Each page represents a physical location.
- "Sessions" = people researching that specific gym location
- High bounce rate on a location page may mean people found what they needed quickly (address, opening hours)
- Compare locations against each other to identify which gyms are attracting the most online interest
- Low traffic to a location page may indicate that gym needs more local SEO or marketing support
""" if is_location_report else ""

        user_prompt = f"""Analyse the following GA4 website analytics data and produce a comprehensive performance report.

{f'BUSINESS CONTEXT: {context}' if context else 'No specific business context provided.'}

{f'SPECIFIC URLs REQUESTED: {", ".join(urls)}' if urls else 'Analyse all pages in the data.'}
{location_context}
The data is structured in three blocks:
- BLOCK 1: Last 7 days — standalone snapshot, no comparison required
- BLOCK 2: Last 28 days vs previous 28 days
- BLOCK 3: Last 28 days vs same period last year

{data_summary}

REPORT STRUCTURE:

1. EXECUTIVE SUMMARY
   - 3-4 sentence overview across all three time blocks
   - Single most important finding
   - Whether things are improving or declining with specific numbers
   {"- Identify top and bottom performing locations by sessions" if is_location_report else ""}

2. LAST 7 DAYS — PERFORMANCE SNAPSHOT
   - Sessions, users, engagement rate, bounce rate, session duration per page
   - Top traffic sources and device split
   - Top geographic locations
   - Plain English box for each finding

3. LAST 28 DAYS vs PREVIOUS 28 DAYS
   - How user numbers are trending vs last month
   - Per-page current vs previous with % change
   - New vs returning users breakdown
   - Acquisition channel trends
   - Lead generation signals
   - Plain English box for each finding

4. LAST 28 DAYS vs SAME PERIOD LAST YEAR
   - Per-page current vs last year with % change
   - Seasonal patterns and long-term trends
   - Note clearly if a page is too new for year-on-year comparison
   - Plain English box for each finding

5. PAGE PERFORMANCE SUMMARY
   - HTML comparison table: Page, Sessions (28d), Bounce Rate, Engagement Rate, Avg Duration, vs Last Month, vs Last Year, Verdict
   - Include ALL pages — do not truncate
   - Plain English box

6. ACTIONABLE RECOMMENDATIONS
   a) DO THIS WEEK
   b) DO THIS MONTH
   c) PLAN FOR NEXT QUARTER
   Each must reference a specific metric and explain exactly what to do.

RULES:
- Every claim must cite a specific number. No vague statements.
- Plain English boxes use: <div style="background-color: #f0f7ff; border-left: 4px solid #4285F4; padding: 12px; margin: 10px 0; border-radius: 4px;">
- Format percentages to 1 decimal place. Numbers with commas for thousands.
- If data is insufficient or anomalous, say so honestly."""

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8000,
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt
        )
        return message.content[0].text

    except Exception as e:
        return f"<p style='color: red;'>Error generating AI insights: {str(e)}</p>"


# ============================================================
# CHART GENERATION
# ============================================================

def generate_charts(all_data):
    charts = {}
    try:
        current_pages = all_data.get("page_performance", {}).get("current", [])
        pages_sorted = sorted(current_pages, key=lambda x: x['sessions'], reverse=True)[:8]

        if pages_sorted:
            page_names = [p['pagePath'][:35] + ('...' if len(p['pagePath']) > 35 else '') for p in pages_sorted]
            sessions = [p['sessions'] for p in pages_sorted]

            fig1 = go.Figure(data=[go.Bar(
                y=page_names, x=sessions, orientation='h',
                marker=dict(color='#4285F4'), text=sessions, textposition='outside'
            )])
            fig1.update_layout(
                title='Sessions by page (last 28 days)', xaxis_title='Sessions',
                yaxis=dict(autorange="reversed"),
                height=max(300, len(pages_sorted) * 50 + 100),
                margin=dict(l=200, r=80, t=60, b=50), font=dict(size=12)
            )
            img = pio.to_image(fig1, format='png', width=800, height=max(300, len(pages_sorted) * 50 + 100))
            charts['sessions_chart'] = base64.b64encode(img).decode()

            bounce_rates = [p['bounceRate'] * 100 for p in pages_sorted]
            colors = ['#EA4335' if br > 60 else '#FBBC04' if br > 40 else '#34A853' for br in bounce_rates]
            fig2 = go.Figure(data=[go.Bar(
                y=page_names, x=bounce_rates, orientation='h',
                marker=dict(color=colors),
                text=[f"{br:.1f}%" for br in bounce_rates], textposition='outside'
            )])
            fig2.update_layout(
                title='Bounce rate by page (%)', xaxis_title='Bounce Rate (%)',
                yaxis=dict(autorange="reversed"),
                height=max(300, len(pages_sorted) * 50 + 100),
                margin=dict(l=200, r=80, t=60, b=50), font=dict(size=12)
            )
            fig2.add_vline(x=50, line_dash="dash", line_color="gray", opacity=0.5)
            img = pio.to_image(fig2, format='png', width=800, height=max(300, len(pages_sorted) * 50 + 100))
            charts['bounce_rate_chart'] = base64.b64encode(img).decode()

            eng_rates = [p['engagementRate'] * 100 for p in pages_sorted]
            eng_colors = ['#34A853' if er > 60 else '#FBBC04' if er > 40 else '#EA4335' for er in eng_rates]
            fig5 = go.Figure(data=[go.Bar(
                y=page_names, x=eng_rates, orientation='h',
                marker=dict(color=eng_colors),
                text=[f"{er:.1f}%" for er in eng_rates], textposition='outside'
            )])
            fig5.update_layout(
                title='Engagement rate by page (%)', xaxis_title='Engagement Rate (%)',
                yaxis=dict(autorange="reversed"),
                height=max(300, len(pages_sorted) * 50 + 100),
                margin=dict(l=200, r=80, t=60, b=50), font=dict(size=12)
            )
            img = pio.to_image(fig5, format='png', width=800, height=max(300, len(pages_sorted) * 50 + 100))
            charts['engagement_chart'] = base64.b64encode(img).decode()

        # Device chart: aggregate across all pages using the separately fetched device data
        device_totals = {}
        for page in current_pages:
            for dev, sess in page.get('devices', {}).items():
                device_totals[dev] = device_totals.get(dev, 0) + sess
        if device_totals:
            fig3 = go.Figure(data=[go.Pie(
                labels=list(device_totals.keys()), values=list(device_totals.values()),
                marker=dict(colors=['#4285F4', '#EA4335', '#FBBC04', '#34A853']),
                textinfo='label+percent', textposition='inside'
            )])
            fig3.update_layout(title='Traffic by device type', height=400,
                               margin=dict(l=50, r=50, t=60, b=50), font=dict(size=13))
            img = pio.to_image(fig3, format='png', width=600, height=400)
            charts['device_chart'] = base64.b64encode(img).decode()

        acq = all_data.get("acquisition", [])
        ch_agg = {}
        for a in acq:
            ch = a['sessionDefaultChannelGroup']
            ch_agg[ch] = ch_agg.get(ch, 0) + a['sessions']
        if ch_agg:
            ch_sorted = sorted(ch_agg.items(), key=lambda x: -x[1])[:8]
            fig4 = go.Figure(data=[go.Bar(
                x=[c[0] for c in ch_sorted], y=[c[1] for c in ch_sorted],
                marker=dict(color='#34A853'),
                text=[c[1] for c in ch_sorted], textposition='outside'
            )])
            fig4.update_layout(
                title='Sessions by acquisition channel', yaxis_title='Sessions',
                height=400, margin=dict(l=60, r=50, t=60, b=80), font=dict(size=12)
            )
            img = pio.to_image(fig4, format='png', width=800, height=400)
            charts['acquisition_chart'] = base64.b64encode(img).decode()

    except Exception as e:
        print(f"Error generating charts: {str(e)}", flush=True)

    return charts


# ============================================================
# MAIN ENHANCED ENDPOINT
# ============================================================

@app.route('/analyze-with-ai', methods=['POST'])
def analyze_with_ai():
    try:
        raw_body = request.get_data(as_text=True)
        raw_body = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', raw_body)
        data = json.loads(raw_body) if raw_body else None

        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400

        claude_api_key = data.get('claude_api_key')
        if not claude_api_key:
            return jsonify({"error": "claude_api_key is required for AI analysis"}), 400

        property_id = data.get('property_id')
        credentials_dict = data.get('credentials')
        if isinstance(credentials_dict, str):
            credentials_dict = json.loads(credentials_dict)
        context = data.get('context', '')

        raw_urls = data.get('urls', [])
        urls = sanitise_url_list(raw_urls)

        print(f"[DEBUG] /analyze-with-ai received | property_id={property_id} | urls={urls}", flush=True)

        if not property_id or not credentials_dict:
            return jsonify({"error": "property_id and credentials are required"}), 400

        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        ga4_client = BetaAnalyticsDataClient(credentials=credentials)

        all_data, periods = collect_all_data(ga4_client, property_id, urls)

        ai_insights = analyze_with_claude(all_data, periods, claude_api_key, urls, context)

        if isinstance(ai_insights, str):
            ai_insights = ai_insights.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            ai_insights = re.sub(r'[\x00-\x1f\x7f]', '', ai_insights)
            ai_insights = re.sub(r' {2,}', ' ', ai_insights)

        charts = generate_charts(all_data)

        now = datetime.now()
        print(f"[DEBUG] /analyze-with-ai complete | returning response", flush=True)
        return jsonify({
            "success": True,
            "property_id": property_id,
            "date_range": {
                "start": (now - timedelta(days=28)).strftime("%Y-%m-%d"),
                "end": now.strftime("%Y-%m-%d")
            },
            "total_pages": len(all_data.get("page_performance", {}).get("current", [])),
            "ga4_data": all_data.get("page_performance", {}).get("current", []),
            "ai_insights": ai_insights,
            "charts": charts
        })

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[DEBUG] /analyze-with-ai ERROR: {str(e)}\n{tb}", flush=True)
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": tb
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
