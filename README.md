# IDOT Bid Intelligence Platform

**Railway Deployment Package - Ready to Deploy**

---

## Quick Deploy (5 minutes)

### 1. Push to GitHub

```bash
git init
git add .
git commit -m "Initial deployment"
git remote add origin https://github.com/YOUR_USERNAME/idot-platform.git
git push -u origin main
```

### 2. Deploy on Railway

1. Go to https://railway.app
2. New Project → Deploy from GitHub repo
3. Select your repository
4. Railway will auto-deploy

### 3. Set Environment Variables

In Railway dashboard, add:

```
DATABASE_PATH=/app/data/idot_intelligence.db
SECRET_KEY=<generate-random-key>
API_KEY=<generate-random-key>
```

Generate keys:
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 4. Upload Database

After deployment, upload the database file (68 MB):

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login and link
railway login
railway link

# Create volume in Railway dashboard at /app/data
# Then upload database (see DATABASE_UPLOAD.md)
```

### 5. Verify

Visit: `https://your-app.railway.app/health`

Should return: `{"status":"healthy"}`

---

## What's Included

- ✅ FastAPI backend
- ✅ Interactive dashboard
- ✅ 14 API endpoints
- ✅ Swagger documentation
- ✅ Production-ready config

## Database

The database (68 MB) must be uploaded separately after deployment.
See included `DATABASE_UPLOAD.md` for instructions.

---

## Files

- `app/` - Application code
- `requirements.txt` - Python dependencies
- `Procfile` - Start command
- `.gitignore` - Excludes database from git
- `.env.example` - Environment template

---

## Support

For deployment issues:
- Railway Docs: https://docs.railway.app
- Railway Discord: https://discord.gg/railway

**This package is configured to work out of the box with Railway!**
