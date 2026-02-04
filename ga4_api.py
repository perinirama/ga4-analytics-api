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

if __name__ == '__main__':
    # For local development
    app.run(host='0.0.0.0', port=5000, debug=True)
