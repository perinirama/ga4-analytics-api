# GA4 Analytics API - Deployment Guide

## What This API Does

This Flask API accepts GA4 credentials dynamically and returns analytics data. It's designed to work with your Make.com automation.

**Endpoint**: `POST /analyze`

**Input**: 
```json
{
  "property_id": "123456789",
  "credentials": { /* full GA4 service account JSON */ },
  "urls": ["https://site.com/page1", "https://site.com/page2"],
  "days_back": 7
}
```

**Output**:
```json
{
  "success": true,
  "data": [
    {
      "pagePath": "/page1",
      "sessions": 150,
      "totalUsers": 120,
      "bounceRate": 0.45,
      "averageSessionDuration": 125.5,
      "engagedSessions": 95
    }
  ]
}
```

---

## Option 1: Deploy to Render (Recommended - Free Tier)

### Step 1: Create GitHub Repository
1. Go to github.com and create a new repository (e.g., "ga4-analytics-api")
2. Upload these files:
   - `ga4_api.py`
   - `requirements.txt`
   - `Procfile`

### Step 2: Deploy on Render
1. Go to https://render.com and sign up (free)
2. Click "New +" → "Web Service"
3. Connect your GitHub account
4. Select your ga4-analytics-api repository
5. Configure:
   - **Name**: ga4-analytics-api
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn ga4_api:app`
   - **Instance Type**: Free
6. Click "Create Web Service"
7. Wait 2-5 minutes for deployment
8. Copy your API URL (looks like: `https://ga4-analytics-api.onrender.com`)

---

## Option 2: Deploy to Railway (Also Free Tier)

### Step 1: Create GitHub Repository (same as above)

### Step 2: Deploy on Railway
1. Go to https://railway.app and sign up (free)
2. Click "New Project" → "Deploy from GitHub repo"
3. Select your repository
4. Railway auto-detects Python and deploys
5. Once deployed, click on your service
6. Go to "Settings" → "Networking" → "Generate Domain"
7. Copy your API URL (looks like: `https://ga4-analytics-api.up.railway.app`)

---

## Option 3: Deploy to PythonAnywhere (Free Tier)

### Step 1: Sign Up
1. Go to https://www.pythonanywhere.com and create free account

### Step 2: Upload Files
1. Go to "Files" tab
2. Create a new directory: `/home/yourusername/ga4api`
3. Upload `ga4_api.py` and `requirements.txt`

### Step 3: Install Dependencies
1. Go to "Consoles" tab
2. Start a Bash console
3. Run:
```bash
cd ga4api
pip3 install --user -r requirements.txt
```

### Step 4: Configure Web App
1. Go to "Web" tab
2. Click "Add a new web app"
3. Choose "Manual configuration" → Python 3.10
4. In "Code" section:
   - **Source code**: `/home/yourusername/ga4api`
   - **Working directory**: `/home/yourusername/ga4api`
5. Edit WSGI configuration file:
```python
import sys
path = '/home/yourusername/ga4api'
if path not in sys.path:
    sys.path.append(path)

from ga4_api import app as application
```
6. Click "Reload" your web app
7. Your API URL: `https://yourusername.pythonanywhere.com`

---

## Testing Your Deployed API

### Test Health Endpoint
```bash
curl https://your-api-url.com/health
```

Should return:
```json
{"status": "healthy", "timestamp": "2026-02-04T..."}
```

### Test with Real Data
Use Postman, Insomnia, or curl:

```bash
curl -X POST https://your-api-url.com/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "property_id": "YOUR_GA4_PROPERTY_ID",
    "credentials": {
      "type": "service_account",
      "project_id": "...",
      "private_key": "...",
      "client_email": "..."
    },
    "urls": ["https://www.rehabandheal.com/lymphatic-drainage-massage"],
    "days_back": 7
  }'
```

---

## Connecting to Make.com

Once deployed, in your Make.com scenario:

1. **Delete the Google Analytics 4 module**
2. **Add HTTP - Make a request module** instead
3. Configure:
   - **URL**: `https://your-api-url.com/analyze`
   - **Method**: POST
   - **Headers**:
     - `Content-Type`: `application/json`
   - **Body**: JSON with:
     ```json
     {
       "property_id": "{{ga4_property_id}}", 
       "credentials": {{parsed_json_output}},
       "urls": ["{{urls}}"],
       "days_back": 7
     }
     ```

4. The response will contain the GA4 data
5. Pass this to Claude for analysis

---

## Troubleshooting

**Error: "Module not found"**
- Make sure all dependencies are installed
- Check Python version is 3.8+

**Error: "Invalid credentials"**
- Verify the service account JSON is complete
- Ensure the service account has Viewer access to the GA4 property

**API returns 500 error**
- Check the logs in your hosting platform
- Verify the property_id is correct
- Ensure credentials JSON is valid

**Connection timeout**
- Free tiers may have cold starts (first request takes 10-30 seconds)
- This is normal - subsequent requests will be faster

---

## Security Notes

- This API doesn't store any credentials
- All authentication happens in-memory per request
- No data is persisted to disk
- Use HTTPS only (all platforms provide this by default)

---

## Next Steps

1. Deploy to one of the platforms above
2. Test the `/health` endpoint
3. Test the `/analyze` endpoint with your Rehab and Heal credentials
4. Update Make.com to call your API instead of GA4 directly
5. Test end-to-end flow

---

## Costs

- **Render Free Tier**: 750 hours/month (plenty for this use case)
- **Railway Free Tier**: $5 credit/month (should be enough for testing)
- **PythonAnywhere Free**: Always free but slower

For production with many users, upgrade to paid tiers (~$7-20/month).
