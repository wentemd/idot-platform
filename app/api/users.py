"""
User management and database models
"""
import sqlite3
import os
from datetime import datetime, timedelta
from typing import Optional
import bcrypt
import secrets


def get_user_db():
    """Get user database connection"""
    db_path = os.getenv("USER_DATABASE_PATH", "/app/data/users.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def init_user_db():
    """Initialize user database tables"""
    conn = get_user_db()
    cursor = conn.cursor()
    
    # Users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT,
            name TEXT,
            google_id TEXT UNIQUE,
            tier TEXT DEFAULT 'free',
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            subscription_status TEXT DEFAULT 'none',
            subscription_end_date TEXT,
            daily_searches INTEGER DEFAULT 0,
            last_search_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Sessions table for token management
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token TEXT UNIQUE NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    conn.commit()
    conn.close()

def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    password_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password_bytes, salt).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    password_bytes = plain_password.encode('utf-8')
    hashed_bytes = hashed_password.encode('utf-8')
    return bcrypt.checkpw(password_bytes, hashed_bytes)

def create_user(email: str, password: Optional[str] = None, name: Optional[str] = None, 
                google_id: Optional[str] = None) -> dict:
    """Create a new user"""
    conn = get_user_db()
    cursor = conn.cursor()
    
    password_hash = hash_password(password) if password else None
    
    try:
        cursor.execute("""
            INSERT INTO users (email, password_hash, name, google_id)
            VALUES (?, ?, ?, ?)
        """, [email, password_hash, name, google_id])
        conn.commit()
        user_id = cursor.lastrowid
        
        cursor.execute("SELECT * FROM users WHERE id = ?", [user_id])
        user = dict(cursor.fetchone())
        conn.close()
        return user
    except sqlite3.IntegrityError:
        conn.close()
        return None

def get_user_by_email(email: str) -> Optional[dict]:
    """Get user by email"""
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE email = ?", [email])
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_user_by_google_id(google_id: str) -> Optional[dict]:
    """Get user by Google ID"""
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE google_id = ?", [google_id])
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_user_by_id(user_id: int) -> Optional[dict]:
    """Get user by ID"""
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", [user_id])
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def update_user(user_id: int, **kwargs) -> bool:
    """Update user fields"""
    conn = get_user_db()
    cursor = conn.cursor()
    
    allowed_fields = ['name', 'tier', 'stripe_customer_id', 'stripe_subscription_id',
                      'subscription_status', 'subscription_end_date', 'daily_searches', 
                      'last_search_date', 'google_id', 'password_hash']
    
    updates = []
    values = []
    for key, value in kwargs.items():
        if key in allowed_fields:
            updates.append(f"{key} = ?")
            values.append(value)
    
    if not updates:
        conn.close()
        return False
    
    values.append(user_id)
    cursor.execute(f"""
        UPDATE users SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, values)
    conn.commit()
    conn.close()
    return True

def create_session(user_id: int, expires_hours: int = 24 * 7) -> str:
    """Create a session token for a user"""
    conn = get_user_db()
    cursor = conn.cursor()
    
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(hours=expires_hours)).isoformat()
    
    cursor.execute("""
        INSERT INTO sessions (user_id, token, expires_at)
        VALUES (?, ?, ?)
    """, [user_id, token, expires_at])
    conn.commit()
    conn.close()
    
    return token

def get_user_by_token(token: str) -> Optional[dict]:
    """Get user by session token"""
    conn = get_user_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT users.* FROM users
        JOIN sessions ON users.id = sessions.user_id
        WHERE sessions.token = ? AND sessions.expires_at > ?
    """, [token, datetime.utcnow().isoformat()])
    
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def delete_session(token: str) -> bool:
    """Delete a session (logout)"""
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sessions WHERE token = ?", [token])
    conn.commit()
    deleted = cursor.rowcount > 0
    conn.close()
    return deleted

def cleanup_expired_sessions():
    """Remove expired sessions"""
    conn = get_user_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sessions WHERE expires_at < ?", [datetime.utcnow().isoformat()])
    conn.commit()
    conn.close()

def check_and_reset_daily_searches(user_id: int) -> int:
    """Check daily search count, reset if new day, return current count"""
    conn = get_user_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT daily_searches, last_search_date FROM users WHERE id = ?", [user_id])
    row = cursor.fetchone()
    
    today = datetime.utcnow().date().isoformat()
    
    if row['last_search_date'] != today:
        # New day, reset counter
        cursor.execute("""
            UPDATE users SET daily_searches = 0, last_search_date = ?
            WHERE id = ?
        """, [today, user_id])
        conn.commit()
        conn.close()
        return 0
    
    conn.close()
    return row['daily_searches']

def increment_daily_searches(user_id: int) -> int:
    """Increment and return new daily search count"""
    conn = get_user_db()
    cursor = conn.cursor()
    
    today = datetime.utcnow().date().isoformat()
    
    cursor.execute("""
        UPDATE users SET daily_searches = daily_searches + 1, last_search_date = ?
        WHERE id = ?
    """, [today, user_id])
    conn.commit()
    
    cursor.execute("SELECT daily_searches FROM users WHERE id = ?", [user_id])
    count = cursor.fetchone()['daily_searches']
    conn.close()
    
    return count

def get_user_limits(user: dict) -> dict:
    """Get rate limits based on user tier"""
    if user['tier'] == 'pro' and user['subscription_status'] == 'active':
        return {
            'daily_searches': 999999,  # Unlimited
            'results_per_query': 500,
            'estimator_access': True,
            'export_access': True
        }
    else:
        return {
            'daily_searches': 15,
            'results_per_query': 50,
            'estimator_access': False,
            'export_access': False
        }
