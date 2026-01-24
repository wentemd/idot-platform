"""
Authentication routes - login, register
"""
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, EmailStr
from typing import Optional

from app.api.users import (
    create_user, get_user_by_email,
    verify_password, create_session, get_user_by_token,
    delete_session, get_user_limits
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
