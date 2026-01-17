"""
API Routes for IDOT Bid Intelligence Platform
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import sqlite3
import os
from datetime import datetime

router = APIRouter()

# Database connection helper
def get_db():
    """Get database connection"""
    db_path = os.getenv("DATABASE_PATH", "./data/idot_intelligence.db")
    return sqlite3.connect(db_path)

# ============================================================================
# SEARCH ENDPOINTS
# ============================================================================

@router.get("/search/pay-item/{pay_code}")
async def search_pay_item(pay_code: str):
    """Get pricing information for a specific pay item"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Get weighted average pricing
        cursor.execute("""
        SELECT pay_item, description, uom, weighted_avg_price, 
               total_quantity, bid_count, min_price, max_price
        FROM weighted_avg_prices
        WHERE pay_item = ?
        """, (pay_code,))
        
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Pay item {pay_code} not found")
        
        # Get 2026 projection if available
        cursor.execute("""
        SELECT projected_2026, yoy_change_pct
        FROM price_inflation
        WHERE pay_item = ?
        """, (pay_code,))
        
        projection = cursor.fetchone()
        
        # Get recent bids
        cursor.execute("""
        SELECT contract_number, letting_date, unit_price, quantity
        FROM item_bids
        WHERE pay_item = ?
        ORDER BY letting_date DESC
        LIMIT 10
        """, (pay_code,))
        
        recent_bids = cursor.fetchall()
        
        conn.close()
        
        return {
            "pay_item": result[0],
            "description": result[1],
            "unit": result[2],
            "pricing": {
                "weighted_average_2025": result[3],
                "weighted_average_2026": projection[0] if projection else None,
                "inflation_factor": projection[1] if projection else None,
                "trend": projection[2] if projection else None,
                "min_price": result[7],
                "max_price": result[8],
                "std_dev": result[6]
            },
            "statistics": {
                "total_quantity": result[4],
                "total_bids": result[5]
            },
            "recent_bids": [
                {
                    "contract": bid[0],
                    "date": bid[1],
                    "unit_price": bid[2],
                    "quantity": bid[3]
                } for bid in recent_bids
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search/contractor/{contractor_name}")
async def search_contractor(
    contractor_name: str,
    limit: int = Query(50, le=200)
):
    """Search for contractor bids"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # IDOT bids
        cursor.execute("""
        SELECT contract_number, contractor_name, letting_date, 
               low_bid_amount, is_low_bidder, number_of_bidders
        FROM contractor_bids
        WHERE contractor_name LIKE ?
        ORDER BY letting_date DESC
        LIMIT ?
        """, (f'%{contractor_name}%', limit))
        
        idot_bids = cursor.fetchall()
        
        # Municipal bids
        cursor.execute("""
        SELECT c.contract_number, b.contractor_name, c.letting_date,
               b.total_bid, b.is_low_bidder, c.total_bids
        FROM municipal_bids b
        JOIN municipal_contracts c ON b.contract_id = c.contract_id
        WHERE b.contractor_name LIKE ?
        ORDER BY c.letting_date DESC
        LIMIT ?
        """, (f'%{contractor_name}%', limit))
        
        municipal_bids = cursor.fetchall()
        
        # Statistics
        cursor.execute("""
        SELECT COUNT(*), SUM(is_low_bidder), AVG(low_bid_amount)
        FROM contractor_bids
        WHERE contractor_name LIKE ?
        """, (f'%{contractor_name}%',))
        
        stats = cursor.fetchone()
        
        conn.close()
        
        return {
            "contractor": contractor_name,
            "statistics": {
                "total_idot_bids": stats[0],
                "idot_wins": stats[1],
                "win_rate": round(stats[1] / stats[0] * 100, 1) if stats[0] > 0 else 0,
                "avg_bid_amount": stats[2],
                "total_municipal_bids": len(municipal_bids)
            },
            "idot_bids": [
                {
                    "contract": bid[0],
                    "contractor": bid[1],
                    "date": bid[2],
                    "amount": bid[3],
                    "won": bool(bid[4]),
                    "bidders": bid[5]
                } for bid in idot_bids
            ],
            "municipal_bids": [
                {
                    "contract": bid[0],
                    "contractor": bid[1],
                    "date": bid[2],
                    "amount": bid[3],
                    "won": bool(bid[4]),
                    "bidders": bid[5]
                } for bid in municipal_bids
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# PRICING ENDPOINTS
# ============================================================================

@router.get("/pricing/weighted-averages")
async def get_weighted_averages(
    skip: int = 0,
    limit: int = Query(100, le=500),
    category: Optional[str] = None
):
    """Get weighted average prices"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if category:
            # Filter by category (would need category field in DB)
            cursor.execute("""
            SELECT pay_item, description, unit, weighted_avg_price, total_bids
            FROM weighted_avg_prices
            LIMIT ? OFFSET ?
            """, (limit, skip))
        else:
            cursor.execute("""
            SELECT pay_item, description, unit, weighted_avg_price, total_bids
            FROM weighted_avg_prices
            ORDER BY total_bids DESC
            LIMIT ? OFFSET ?
            """, (limit, skip))
        
        results = cursor.fetchall()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM weighted_avg_prices")
        total = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "items": [
                {
                    "pay_item": r[0],
                    "description": r[1],
                    "unit": r[2],
                    "weighted_avg_price": r[3],
                    "total_bids": r[4]
                } for r in results
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pricing/inflation-2026")
async def get_inflation_projections(
    trend: Optional[str] = None,
    limit: int = Query(100, le=500)
):
    """Get 2026 inflation projections"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if trend and trend.upper() in ['RISING', 'STABLE', 'FALLING']:
            cursor.execute("""
            SELECT pay_item, description, weighted_avg_2025, weighted_avg_2026,
                   price_increase_pct, trend_category
            FROM weighted_avg_2026
            WHERE trend_category = ?
            ORDER BY price_increase_pct DESC
            LIMIT ?
            """, (trend.upper(), limit))
        else:
            cursor.execute("""
            SELECT pay_item, description, weighted_avg_2025, weighted_avg_2026,
                   price_increase_pct, trend_category
            FROM weighted_avg_2026
            ORDER BY price_increase_pct DESC
            LIMIT ?
            """, (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        return {
            "projections": [
                {
                    "pay_item": r[0],
                    "description": r[1],
                    "price_2025": r[2],
                    "price_2026": r[3],
                    "increase_pct": r[4],
                    "trend": r[5]
                } for r in results
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# ANALYTICS ENDPOINTS
# ============================================================================

@router.get("/analytics/summary")
async def get_analytics_summary():
    """Get platform analytics summary"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        stats = {}
        
        # IDOT stats
        cursor.execute("SELECT COUNT(*) FROM contracts")
        stats['total_contracts'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM contractor_bids")
        stats['total_bids'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT contractor_name) FROM contractor_bids")
        stats['unique_contractors'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM weighted_avg_prices")
        stats['pay_items'] = cursor.fetchone()[0]
        
        # Municipal stats
        cursor.execute("SELECT COUNT(*) FROM municipal_contracts")
        stats['municipal_contracts'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM municipal_item_bids")
        stats['municipal_bids'] = cursor.fetchone()[0]
        
        # Price trends
        cursor.execute("""
        SELECT trend_category, COUNT(*)
        FROM weighted_avg_2026
        GROUP BY trend_category
        """)
        
        trends = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            "platform_stats": stats,
            "price_trends_2026": trends,
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/contractors/top")
async def get_top_contractors(limit: int = Query(20, le=50)):
    """Get top contractors by bid volume"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        SELECT 
            contractor_name,
            COUNT(*) as total_bids,
            SUM(is_low_bidder) as wins,
            AVG(low_bid_amount) as avg_bid
        FROM contractor_bids
        GROUP BY contractor_name
        HAVING COUNT(*) >= 5
        ORDER BY total_bids DESC
        LIMIT ?
        """, (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        return {
            "top_contractors": [
                {
                    "contractor": r[0],
                    "total_bids": r[1],
                    "wins": r[2],
                    "win_rate": round(r[2] / r[1] * 100, 1),
                    "avg_bid": r[3]
                } for r in results
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# MUNICIPAL ENDPOINTS
# ============================================================================

@router.get("/municipal/contracts")
async def get_municipal_contracts(
    agency: Optional[str] = None,
    limit: int = Query(50, le=200)
):
    """Get municipal contracts"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if agency:
            cursor.execute("""
            SELECT contract_number, project_name, letting_date,
                   total_bids, low_bid_amount, winning_contractor
            FROM municipal_contracts
            WHERE agency_id = ?
            ORDER BY letting_date DESC
            LIMIT ?
            """, (agency, limit))
        else:
            cursor.execute("""
            SELECT contract_number, project_name, letting_date,
                   total_bids, low_bid_amount, winning_contractor
            FROM municipal_contracts
            ORDER BY letting_date DESC
            LIMIT ?
            """, (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        return {
            "contracts": [
                {
                    "contract_number": r[0],
                    "project_name": r[1],
                    "letting_date": r[2],
                    "total_bids": r[3],
                    "low_bid": r[4],
                    "winner": r[5]
                } for r in results
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
