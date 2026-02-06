from flask import Flask, request, jsonify
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
)
from google.oauth2 import service_account
import json
from datetime import datetime, timedelta
import anthropic
import os
import plotly.graph_objects as go
import plotly.io as pio
import base64

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

@app.route('/analyze', methods=['POST'])
def analyze_ga4():
    """
    Main endpoint for GA4 analysis
    
    Expected JSON payload:
    {
        "property_id": "123456789",
        "credentials": {
            "type": "service_account",
            "project_id": "...",
            "private_key_id": "...",
            "private_key": "...",
            "client_email": "...",
            "client_id": "...",
            "auth_uri": "...",
            "token_uri": "...",
            "auth_provider_x509_cert_url": "...",
            "client_x509_cert_url": "..."
        },
        "urls": ["https://example.com/page1", "https://example.com/page2"],
        "days_back": 7
    }
    """
    
    try:
        # Parse request
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400
        
        # Validate required fields
        property_id = data.get('property_id')
        credentials_dict = data.get('credentials')
        urls = data.get('urls', [])
        days_back = data.get('days_back', 7)
        
        if not property_id:
            return jsonify({"error": "property_id is required"}), 400
        
        if not credentials_dict:
            return jsonify({"error": "credentials are required"}), 400
        
        # Create credentials object from the uploaded JSON
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        
        # Initialize GA4 client with dynamic credentials
        client = BetaAnalyticsDataClient(credentials=credentials)
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Build the request
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
        
        # Run the report
        response = client.run_report(request_params)
        
        # Format the response
        results = []
        for row in response.rows:
            result = {
                "pagePath": row.dimension_values[0].value,
                "deviceCategory": row.dimension_values[1].value,
                "sessions": int(row.metric_values[0].value),
                "totalUsers": int(row.metric_values[1].value),
                "bounceRate": float(row.metric_values[2].value),
                "averageSessionDuration": float(row.metric_values[3].value),
                "engagedSessions": int(row.metric_values[4].value),
            }
            results.append(result)
        
        # Filter by URLs if provided
        if urls:
            # Extract paths from full URLs
            url_paths = []
            for url in urls:
                if url:
                    # Extract path from URL
                    from urllib.parse import urlparse
                    parsed = urlparse(url)
                    path = parsed.path if parsed.path else '/'
                    url_paths.append(path)
            
            # Filter results to only include requested paths
            if url_paths:
                filtered_results = [
                    r for r in results 
                    if any(r['pagePath'] == path or r['pagePath'].startswith(path) for path in url_paths)
                ]
                results = filtered_results if filtered_results else results
        
        # Aggregate data by page (combining device categories)
        aggregated = {}
        for row in results:
            path = row['pagePath']
            if path not in aggregated:
                aggregated[path] = {
                    "pagePath": path,
                    "sessions": 0,
                    "totalUsers": 0,
                    "bounceRate": 0,
                    "averageSessionDuration": 0,
                    "engagedSessions": 0,
                    "devices": []
                }
            
            aggregated[path]["sessions"] += row["sessions"]
            aggregated[path]["totalUsers"] += row["totalUsers"]
            aggregated[path]["engagedSessions"] += row["engagedSessions"]
            aggregated[path]["devices"].append({
                "device": row["deviceCategory"],
                "sessions": row["sessions"]
            })
        
        # Calculate weighted averages for rate metrics
        for path, data in aggregated.items():
            total_sessions = data["sessions"]
            if total_sessions > 0:
                # Weighted average bounce rate
                weighted_bounce = sum(
                    r["bounceRate"] * r["sessions"] 
                    for r in results if r["pagePath"] == path
                )
                data["bounceRate"] = round(weighted_bounce / total_sessions, 4)
                
                # Weighted average session duration
                weighted_duration = sum(
                    r["averageSessionDuration"] * r["sessions"] 
                    for r in results if r["pagePath"] == path
                )
                data["averageSessionDuration"] = round(weighted_duration / total_sessions, 2)
        
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

def analyze_with_claude(ga4_data, claude_api_key, urls=None, context=None):
    """
    Send GA4 data to Claude for analysis
    
    Args:
        ga4_data: Dictionary containing GA4 analytics data
        claude_api_key: Claude API key
        urls: Optional list of URLs being analyzed
        context: Optional business context
    
    Returns:
        Claude's analysis as a string
    """
    try:
        client = anthropic.Anthropic(api_key=claude_api_key)
        
        # Format the GA4 data nicely
        data_summary = f"""
GA4 Analytics Data Summary:
Date Range: {ga4_data['date_range']['start']} to {ga4_data['date_range']['end']}
Total Pages Analyzed: {ga4_data['total_pages']}

Page Performance:
"""
        
        for page in ga4_data['data']:
            data_summary += f"""
- {page['pagePath']}
  Sessions: {page['sessions']}
  Users: {page['totalUsers']}
  Bounce Rate: {page['bounceRate']:.1%}
  Avg Session Duration: {page['averageSessionDuration']:.1f}s
  Engaged Sessions: {page['engagedSessions']}
"""
        
        # Build the prompt
        prompt = f"""You are a UX and digital marketing analytics expert analyzing website data for a wellness and therapy clinic.

{data_summary}"""
        
        if urls:
            prompt += f"\n\nURLs requested for analysis: {', '.join(urls)}"
        
        if context:
            prompt += f"\n\nBusiness context: {context}"
        
        prompt += """

Please provide:
1. Key findings about user behavior and traffic patterns
2. 5 specific, actionable recommendations to improve UX and conversions
3. Any concerning trends or opportunities
4. Quick wins that could be implemented immediately

Format your response as a clear, professional email report suitable for a marketing manager."""
        
        # Call Claude
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract text from response
        analysis = message.content[0].text
        
        return analysis
    
    except Exception as e:
        return f"Error analyzing with Claude: {str(e)}"

def generate_charts(ga4_data):
    """
    Generate charts from GA4 data using Plotly and return as base64 encoded images
    
    Args:
        ga4_data: Dictionary containing GA4 analytics data
        
    Returns:
        Dictionary with base64 encoded chart images
    """
    charts = {}
    
    try:
        # Extract data
        pages = [page['pagePath'] for page in ga4_data['data'][:5]]  # Top 5 pages
        sessions = [page['sessions'] for page in ga4_data['data'][:5]]
        bounce_rates = [page['bounceRate'] * 100 for page in ga4_data['data'][:5]]
        
        # Truncate long page paths for better display
        pages = [p[:30] + '...' if len(p) > 30 else p for p in pages]
        
        # Chart 1: Sessions by Page (Horizontal Bar Chart)
        fig1 = go.Figure(data=[
            go.Bar(
                y=pages,
                x=sessions,
                orientation='h',
                marker=dict(color='#4285F4')
            )
        ])
        fig1.update_layout(
            title='Sessions by Page',
            xaxis_title='Sessions',
            yaxis=dict(autorange="reversed"),
            height=400,
            margin=dict(l=150, r=50, t=80, b=50)
        )
        
        # Convert to base64
        img_bytes1 = pio.to_image(fig1, format='png', width=800, height=400)
        charts['sessions_chart'] = base64.b64encode(img_bytes1).decode()
        
        # Chart 2: Bounce Rate by Page (Horizontal Bar Chart with color coding)
        colors = ['#EA4335' if br > 60 else '#FBBC04' if br > 40 else '#34A853' for br in bounce_rates]
        
        fig2 = go.Figure(data=[
            go.Bar(
                y=pages,
                x=bounce_rates,
                orientation='h',
                marker=dict(color=colors)
            )
        ])
        fig2.update_layout(
            title='Bounce Rate by Page (%)',
            xaxis_title='Bounce Rate (%)',
            yaxis=dict(autorange="reversed"),
            height=400,
            margin=dict(l=150, r=50, t=80, b=50)
        )
        fig2.add_vline(x=50, line_dash="dash", line_color="gray", opacity=0.5)
        
        # Convert to base64
        img_bytes2 = pio.to_image(fig2, format='png', width=800, height=400)
        charts['bounce_rate_chart'] = base64.b64encode(img_bytes2).decode()
        
        # Chart 3: Device Breakdown (Pie Chart)
        device_sessions = {}
        for page in ga4_data['data']:
            for device in page['devices']:
                device_name = device['device']
                device_sessions[device_name] = device_sessions.get(device_name, 0) + device['sessions']
        
        if device_sessions:
            fig3 = go.Figure(data=[
                go.Pie(
                    labels=list(device_sessions.keys()),
                    values=list(device_sessions.values()),
                    marker=dict(colors=['#4285F4', '#EA4335', '#FBBC04', '#34A853'])
                )
            ])
            fig3.update_layout(
                title='Traffic by Device Type',
                height=400,
                margin=dict(l=50, r=50, t=80, b=50)
            )
            
            # Convert to base64
            img_bytes3 = pio.to_image(fig3, format='png', width=600, height=400)
            charts['device_chart'] = base64.b64encode(img_bytes3).decode()
        
        return charts
        
    except Exception as e:
        print(f"Error generating charts: {str(e)}")
        return {}


@app.route('/analyze-with-ai', methods=['POST'])
def analyze_with_ai():
    """
    Enhanced endpoint that includes Claude AI analysis
    
    Expected JSON payload:
    {
        "property_id": "123456789",
        "credentials": { GA4 service account JSON },
        "urls": ["https://example.com/page1"],
        "days_back": 7,
        "claude_api_key": "sk-ant-...",
        "context": "Optional business context"
    }
    """
    
    try:
        # Get the basic analyze response
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400
        
        # Extract Claude API key
        claude_api_key = data.get('claude_api_key')
        if not claude_api_key:
            return jsonify({"error": "claude_api_key is required for AI analysis"}), 400
        
        # Get GA4 data first (reuse the existing logic)
        property_id = data.get('property_id')
        credentials_dict = data.get('credentials')
        urls = data.get('urls', [])
        days_back = data.get('days_back', 7)
        context = data.get('context', '')
        
        if not property_id or not credentials_dict:
            return jsonify({"error": "property_id and credentials are required"}), 400
        
        # Get GA4 credentials
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        
        # Initialize GA4 client
        client = BetaAnalyticsDataClient(credentials=credentials)
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Build and run GA4 request
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
        
        # Format results (same as before)
        results = []
        for row in response.rows:
            result = {
                "pagePath": row.dimension_values[0].value,
                "deviceCategory": row.dimension_values[1].value,
                "sessions": int(row.metric_values[0].value),
                "totalUsers": int(row.metric_values[1].value),
                "bounceRate": float(row.metric_values[2].value),
                "averageSessionDuration": float(row.metric_values[3].value),
                "engagedSessions": int(row.metric_values[4].value),
            }
            results.append(result)
        
        # Filter and aggregate (same as before)
        if urls:
            from urllib.parse import urlparse
            url_paths = []
            for url in urls:
                if url:
                    parsed = urlparse(url)
                    path = parsed.path if parsed.path else '/'
                    url_paths.append(path)
            
            if url_paths:
                filtered_results = [
                    r for r in results 
                    if any(r['pagePath'] == path or r['pagePath'].startswith(path) for path in url_paths)
                ]
                results = filtered_results if filtered_results else results
        
        # Aggregate by page
        aggregated = {}
        for row in results:
            path = row['pagePath']
            if path not in aggregated:
                aggregated[path] = {
                    "pagePath": path,
                    "sessions": 0,
                    "totalUsers": 0,
                    "bounceRate": 0,
                    "averageSessionDuration": 0,
                    "engagedSessions": 0,
                    "devices": []
                }
            
            aggregated[path]["sessions"] += row["sessions"]
            aggregated[path]["totalUsers"] += row["totalUsers"]
            aggregated[path]["engagedSessions"] += row["engagedSessions"]
            aggregated[path]["devices"].append({
                "device": row["deviceCategory"],
                "sessions": row["sessions"]
            })
        
        # Calculate weighted averages
        for path, data in aggregated.items():
            total_sessions = data["sessions"]
            if total_sessions > 0:
                weighted_bounce = sum(
                    r["bounceRate"] * r["sessions"] 
                    for r in results if r["pagePath"] == path
                )
                data["bounceRate"] = round(weighted_bounce / total_sessions, 4)
                
                weighted_duration = sum(
                    r["averageSessionDuration"] * r["sessions"] 
                    for r in results if r["pagePath"] == path
                )
                data["averageSessionDuration"] = round(weighted_duration / total_sessions, 2)
        
        # Prepare GA4 data for Claude
        ga4_data = {
            "property_id": property_id,
            "date_range": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d")
            },
            "total_pages": len(aggregated),
            "data": list(aggregated.values())
        }
        
        # Get Claude analysis
        ai_insights = analyze_with_claude(ga4_data, claude_api_key, urls, context)
        
        # Generate charts
        charts = generate_charts(ga4_data)
        
        # Return combined response
        return jsonify({
            "success": True,
            "property_id": property_id,
            "date_range": ga4_data["date_range"],
            "total_pages": len(aggregated),
            "ga4_data": list(aggregated.values()),
            "ai_insights": ai_insights,
            "charts": charts
        })
    
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }), 500

if __name__ == '__main__':
    # For local development
    app.run(host='0.0.0.0', port=5000, debug=True)
