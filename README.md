# Twilize Workbook Service

Generates Tableau `.twbx` workbooks from a normalized `TableauSpec` JSON.

## Deploy to Railway

1. **Create a new Railway project** at [railway.app](https://railway.app)
2. **Connect this folder** as a GitHub repo, or use the Railway CLI:
   ```bash
   railway login
   railway init
   railway up
   ```
3. Railway will auto-detect the `Dockerfile` and deploy.
4. Copy the generated service URL (e.g. `https://twilize-service-production-xxxx.up.railway.app`)

## Set the secret in Lovable

In your Lovable project, set the `TWILIZE_SERVICE_URL` secret to the Railway URL (without trailing slash).

The existing `twb-generator` edge function will automatically start forwarding specs to this service.

## API

### `GET /health`
Returns `{ "status": "ok", "twilize_available": true }`.

### `POST /generate`
**Request body:**
```json
{
  "spec": { "title": "...", "pages": [...], "columns": [...], "charts": [...], "kpis": [...], "filters": [...], "brand": {...}, "sampleRowCount": 5 },
  "sample_csv": "Category,Date,Value\nA,2024-01-01,100\n..."
}
```

**Response:**
```json
{ "twbx_base64": "UEsDBBQAAAA..." }
```

## Local development

```bash
pip install -r requirements.txt
python app.py
# Service runs at http://localhost:8080
```
