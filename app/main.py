"""
IDOT Bid Intelligence Platform - Main Application
FastAPI backend for bid pricing intelligence
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
from pathlib import Path

# Import API routes
from app.api import routes

# Initialize FastAPI app
app = FastAPI(
    title="IDOT Bid Intelligence Platform",
    description="Comprehensive bid pricing intelligence for IDOT and municipal construction projects",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Templates
templates_path = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_path))

# Include API routes
app.include_router(routes.router, prefix="/api", tags=["api"])

# Root endpoint - serve dashboard
@app.get("/")
async def root(request: Request):
    """Serve the main dashboard"""
    return templates.TemplateResponse("index.html", {"request": request})

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for Railway/monitoring"""
    return {
        "status": "healthy",
        "service": "IDOT Bid Intelligence Platform",
        "version": "1.0.0"
    }

# Database status endpoint
@app.get("/api/status")
async def database_status():
    """Check database connectivity and stats"""
    import sqlite3
    
    db_path = os.getenv("DATABASE_PATH", "./data/idot_intelligence.db")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get table counts
        stats = {}
        
        tables = [
            'contracts',
            'contractor_bids', 
            'item_bids',
            'weighted_avg_prices',
            'municipal_contracts',
            'municipal_bids',
            'municipal_item_bids'
        ]
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats[table] = count
            except:
                stats[table] = 0
        
        conn.close()
        
        return {
            "status": "connected",
            "database": db_path,
            "statistics": stats,
            "total_records": sum(stats.values())
        }
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(e)
            }
        )

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Handle 404 errors"""
    return JSONResponse(
        status_code=404,
        content={"error": "Not found", "path": str(request.url)}
    )

@app.exception_handler(500)
async def server_error_handler(request: Request, exc):
    """Handle 500 errors"""
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
