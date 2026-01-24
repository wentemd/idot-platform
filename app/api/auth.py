"""
Authentication routes - login, register, Google OAuth
"""
from fastapi import APIRouter, HTTPException, Request, Response, Depends
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, EmailStr
from typing import Optional
import os
import httpx

from app.api.users import (
    create_user, get_user_by_email, get_user_by_google_id,
    verify_password, create_session, get_user_by_token,
    delete_session, get_user_limits, update_user
)

router = APIRouter()

# Pydantic models
class UserRegister(BaseModel):
    email: EmailStr
    password: str
    name: Optional[str] = None

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class GoogleAuthCallback(BaseModel):
    code: str
    redirect_uri: str

# Environment variables
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
FRONTEND_URL = os.getenv("FRONTEND_URL", "https://idot-platform.onrender.com")


def get_current_user(request: Request) -> Optional[dict]:
    """Extract current user from session token in cookie"""
    token = request.cookies.get("session_token")
    if not token:
        return None
    return get_user_by_token(token)


def require_auth(request: Request) -> dict:
    """Dependency that requires authentication"""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def require_pro(request: Request) -> dict:
    """Dependency that requires Pro tier"""
    user = require_auth(request)
    limits = get_user_limits(user)
    if not limits['estimator_access']:
        raise HTTPException(
            status_code=403, 
            detail="This feature requires a Pro subscription"
        )
    return user


# ============================================================================
# EMAIL/PASSWORD AUTH
# ============================================================================

@router.post("/register")
async def register(user_data: UserRegister, response: Response):
    """Register a new user with email/password"""
    # Check if user exists
    existing = get_user_by_email(user_data.email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Validate password
    if len(user_data.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    
    # Create user
    user = create_user(
        email=user_data.email,
        password=user_data.password,
        name=user_data.name
    )
    
    if not user:
        raise HTTPException(status_code=500, detail="Failed to create user")
    
    # Create session
    token = create_session(user['id'])
    
    # Set cookie
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=7 * 24 * 60 * 60  # 7 days
    )
    
    return {
        "message": "Registration successful",
        "user": {
            "id": user['id'],
            "email": user['email'],
            "name": user['name'],
            "tier": user['tier']
        }
    }


@router.post("/login")
async def login(user_data: UserLogin, response: Response):
    """Login with email/password"""
    user = get_user_by_email(user_data.email)
    
    if not user or not user['password_hash']:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    
    if not verify_password(user_data.password, user['password_hash']):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    
    # Create session
    token = create_session(user['id'])
    
    # Set cookie
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=7 * 24 * 60 * 60  # 7 days
    )
    
    return {
        "message": "Login successful",
        "user": {
            "id": user['id'],
            "email": user['email'],
            "name": user['name'],
            "tier": user['tier'],
            "subscription_status": user['subscription_status']
        }
    }


@router.post("/logout")
async def logout(request: Request, response: Response):
    """Logout - clear session"""
    token = request.cookies.get("session_token")
    if token:
        delete_session(token)
    
    response.delete_cookie("session_token")
    return {"message": "Logged out successfully"}


@router.get("/me")
async def get_me(request: Request):
    """Get current user info"""
    user = get_current_user(request)
    if not user:
        return {"user": None}
    
    limits = get_user_limits(user)
    
    return {
        "user": {
            "id": user['id'],
            "email": user['email'],
            "name": user['name'],
            "tier": user['tier'],
            "subscription_status": user['subscription_status'],
            "subscription_end_date": user['subscription_end_date'],
            "daily_searches": user['daily_searches'],
            "limits": limits
        }
    }


# ============================================================================
# GOOGLE OAUTH
# ============================================================================

@router.get("/google/login")
async def google_login(request: Request):
    """Redirect to Google OAuth"""
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=500, detail="Google OAuth not configured")
    
    # Get the redirect URI from the request or use default
    redirect_uri = f"{FRONTEND_URL}/api/auth/google/callback"
    
    google_auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        f"client_id={GOOGLE_CLIENT_ID}&"
        f"redirect_uri={redirect_uri}&"
        "response_type=code&"
        "scope=email profile&"
        "access_type=offline"
    )
    
    return RedirectResponse(url=google_auth_url)


@router.get("/google/callback")
async def google_callback(request: Request, response: Response, code: str = None, error: str = None):
    """Handle Google OAuth callback"""
    if error:
        return RedirectResponse(url=f"{FRONTEND_URL}?error=google_auth_failed")
    
    if not code:
        return RedirectResponse(url=f"{FRONTEND_URL}?error=no_code")
    
    redirect_uri = f"{FRONTEND_URL}/api/auth/google/callback"
    
    # Exchange code for tokens
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri
            }
        )
    
    if token_response.status_code != 200:
        return RedirectResponse(url=f"{FRONTEND_URL}?error=token_exchange_failed")
    
    tokens = token_response.json()
    access_token = tokens.get("access_token")
    
    # Get user info from Google
    async with httpx.AsyncClient() as client:
        userinfo_response = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"}
        )
    
    if userinfo_response.status_code != 200:
        return RedirectResponse(url=f"{FRONTEND_URL}?error=userinfo_failed")
    
    google_user = userinfo_response.json()
    google_id = google_user.get("id")
    email = google_user.get("email")
    name = google_user.get("name")
    
    # Find or create user
    user = get_user_by_google_id(google_id)
    
    if not user:
        # Check if email exists (user registered with email, now using Google)
        user = get_user_by_email(email)
        if user:
            # Link Google account to existing user
            update_user(user['id'], google_id=google_id)
            user = get_user_by_email(email)
        else:
            # Create new user
            user = create_user(email=email, name=name, google_id=google_id)
    
    if not user:
        return RedirectResponse(url=f"{FRONTEND_URL}?error=user_creation_failed")
    
    # Create session
    token = create_session(user['id'])
    
    # Redirect back to frontend with cookie set
    redirect_response = RedirectResponse(url=f"{FRONTEND_URL}?login=success")
    redirect_response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=7 * 24 * 60 * 60  # 7 days
    )
    
    return redirect_response
