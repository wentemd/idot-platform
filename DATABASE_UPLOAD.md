# Database Upload Instructions

The database file (68 MB) is too large for GitHub.
Upload it separately to Railway after deployment.

---

## Quick Method: Railway CLI

```bash
# 1. Install CLI
npm install -g @railway/cli

# 2. Login
railway login

# 3. Link to project
railway link

# 4. Create volume in Railway dashboard
#    - Go to your service
#    - Click "+ New" → "Volume"
#    - Mount path: /app/data

# 5. Upload database
#    Use Railway shell and base64 upload
#    (detailed instructions in Railway docs)
```

---

## Verify Database

After upload, check:

```
https://your-app.railway.app/api/status
```

Should show record counts if database is connected.

---

## Troubleshooting

If `/api/status` shows error:
1. Check volume is mounted at `/app/data`
2. Check file is named `idot_intelligence.db`
3. Check environment variable `DATABASE_PATH=/app/data/idot_intelligence.db`
