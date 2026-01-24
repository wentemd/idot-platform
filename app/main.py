"""
IDOT Bid Intelligence Platform - Main Application
FastAPI backend with security hardening and user authentication
"""
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
import os
from pathlib import Path

# Import API routes
from app.api import routes
from app.api import auth
from app.api import stripe_routes
from app.api.users import init_user_db

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address, default_limits=["200 per minute"])

# Initialize FastAPI app
app = FastAPI(
    title="IDOT Bid Intelligence Platform",
    description="Construction bid pricing intelligence for IDOT projects",
    version="2.0.0",
    docs_url="/docs" if os.getenv("ENABLE_DOCS", "false").lower() == "true" else None,
    redoc_url="/redoc" if os.getenv("ENABLE_DOCS", "false").lower() == "true" else None,
)

# Initialize user database on startup
@app.on_event("startup")
async def startup_event():
    init_user_db()

# Add rate limiter to app state and middleware
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# CORS middleware - restrict to your domain in production
allowed_origins = os.getenv("ALLOWED_ORIGINS", "").split(",")
if not allowed_origins or allowed_origins == [""]:
    allowed_origins = [
        "https://idot-platform.onrender.com",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Security headers middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response

# Request size limiting middleware
@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > 10 * 1024 * 1024:
        return JSONResponse(
            status_code=413,
            content={"detail": "Request too large. Maximum 10MB allowed."}
        )
    return await call_next(request)

# Include API routes
app.include_router(routes.router, prefix="/api")
app.include_router(auth.router, prefix="/api/auth")
app.include_router(stripe_routes.router, prefix="/api/stripe")

# Setup templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))

# Root route - serve main page
@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Health check (not rate limited)
@app.get("/health")
async def health():
    return {"status": "healthy"}
