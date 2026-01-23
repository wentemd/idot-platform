"""
API Routes for IDOT Bid Intelligence Platform
Updated for flat 'bids' table schema
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
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    return conn

# ============================================================================
# SEARCH ENDPOINTS
# ============================================================================

@router.get("/search/pay-item/{pay_code}")
async def search_pay_item(pay_code: str):
    """Get pricing information for a specific pay item"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Get item info and weighted average pricing
        cursor.execute("""
        SELECT 
            item_number,
            item_description,
            unit,
            SUM(unit_price * quantity) / SUM(quantity) as weighted_avg_price,
            SUM(quantity) as total_quantity,
            COUNT(*) as bid_count,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price,
            AVG(unit_price) as avg_price
        FROM bids
        WHERE item_number LIKE ?
        GROUP BY item_number, item_description, unit
        """, (f'%{pay_code}%',))
        
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Pay item {pay_code} not found")
        
        # Get recent bids for this item
        cursor.execute("""
        SELECT contract_number, letting_date, county, bidder_name, 
               unit_price, quantity, extension, is_low_item, item_rank
        FROM bids
        WHERE item_number LIKE ?
        ORDER BY letting_date DESC
        LIMIT 20
        """, (f'%{pay_code}%',))
        recent_bids = cursor.fetchall()
        
        # Get price by year
        cursor.execute("""
        SELECT 
            SUBSTR(letting_date, 7, 4) as year,
            AVG(unit_price) as avg_price,
            COUNT(*) as bid_count
        FROM bids
        WHERE item_number LIKE ?
        GROUP BY SUBSTR(letting_date, 7, 4)
        ORDER BY year DESC
        """, (f'%{pay_code}%',))
        yearly_prices = cursor.fetchall()
        
        conn.close()
        
        return {
            "pay_item": result['item_number'],
            "description": result['item_description'],
            "unit": result['unit'],
            "pricing": {
                "weighted_average": round(result['weighted_avg_price'], 2) if result['weighted_avg_price'] else None,
                "simple_average": round(result['avg_price'], 2) if result['avg_price'] else None,
                "min_price": result['min_price'],
                "max_price": result['max_price']
            },
            "statistics": {
                "total_quantity": result['total_quantity'],
                "total_bids": result['bid_count']
            },
            "yearly_prices": [
                {
                    "year": row['year'],
                    "avg_price": round(row['avg_price'], 2),
                    "bid_count": row['bid_count']
                } for row in yearly_prices
            ],
            "recent_bids": [
                {
                    "contract": row['contract_number'],
                    "date": row['letting_date'],
                    "county": row['county'],
                    "bidder": row['bidder_name'],
                    "unit_price": row['unit_price'],
                    "quantity": row['quantity'],
                    "extension": row['extension'],
                    "was_low": row['is_low_item'] == 'Y',
                    "rank": row['item_rank']
                } for row in recent_bids
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        conn.close()
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
        # Get contractor's contract-level bids
        cursor.execute("""
        SELECT DISTINCT
            contract_number,
            letting_date,
            county,
            bidder_name,
            bidder_number,
            total_bid_amount,
            bidder_rank,
            is_winner,
            num_bidders,
            bid_spread_pct
        FROM bids
        WHERE bidder_name LIKE ?
        ORDER BY letting_date DESC
        LIMIT ?
        """, (f'%{contractor_name}%', limit))
        
        bids = cursor.fetchall()
        
        # Get statistics
        cursor.execute("""
        SELECT 
            COUNT(DISTINCT contract_number) as total_contracts,
            SUM(CASE WHEN is_winner = 'Y' THEN 1 ELSE 0 END) as wins,
            AVG(total_bid_amount) as avg_bid,
            AVG(bid_spread_pct) as avg_spread
        FROM (
            SELECT DISTINCT contract_number, is_winner, total_bid_amount, bid_spread_pct
            FROM bids
            WHERE bidder_name LIKE ?
        )
        """, (f'%{contractor_name}%',))
        
        stats = cursor.fetchone()
        
        # Get item-level performance
        cursor.execute("""
        SELECT 
            COUNT(*) as total_items,
            SUM(CASE WHEN is_low_item = 'Y' THEN 1 ELSE 0 END) as low_item_count
        FROM bids
        WHERE bidder_name LIKE ?
        """, (f'%{contractor_name}%',))
        
        item_stats = cursor.fetchone()
        
        conn.close()
        
        total_contracts = stats['total_contracts'] or 0
        wins = stats['wins'] or 0
        
        return {
            "contractor": contractor_name,
            "statistics": {
                "total_contracts": total_contracts,
                "wins": wins,
                "win_rate": round(wins / total_contracts * 100, 1) if total_contracts > 0 else 0,
                "avg_bid_amount": round(stats['avg_bid'], 2) if stats['avg_bid'] else None,
                "avg_spread_pct": round(stats['avg_spread'], 2) if stats['avg_spread'] else None,
                "total_item_bids": item_stats['total_items'],
                "low_item_bids": item_stats['low_item_count'],
                "item_win_rate": round(item_stats['low_item_count'] / item_stats['total_items'] * 100, 1) if item_stats['total_items'] > 0 else 0
            },
            "bids": [
                {
                    "contract": row['contract_number'],
                    "date": row['letting_date'],
                    "county": row['county'],
                    "bidder": row['bidder_name'],
                    "bidder_id": row['bidder_number'],
                    "amount": row['total_bid_amount'],
                    "rank": row['bidder_rank'],
                    "won": row['is_winner'] == 'Y',
                    "num_bidders": row['num_bidders'],
                    "spread_pct": row['bid_spread_pct']
                } for row in bids
            ]
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search/contract/{contract_number}")
async def search_contract(contract_number: str):
    """Get all bids for a specific contract"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Get contract info
        cursor.execute("""
        SELECT DISTINCT
            contract_number, letting_date, district, county, 
            municipality, project_number, section, state_job_number,
            num_bidders, engineers_estimate
        FROM bids
        WHERE contract_number = ?
        LIMIT 1
        """, (contract_number,))
        
        contract = cursor.fetchone()
        
        if not contract:
            raise HTTPException(status_code=404, detail=f"Contract {contract_number} not found")
        
        # Get all bidders on this contract
        cursor.execute("""
        SELECT DISTINCT
            bidder_number, bidder_name, bidder_rank, 
            total_bid_amount, bid_spread_pct, is_winner
        FROM bids
        WHERE contract_number = ?
        ORDER BY bidder_rank
        """, (contract_number,))
        
        bidders = cursor.fetchall()
        
        # Get all items and bids
        cursor.execute("""
        SELECT 
            item_number, item_description, quantity, unit,
            bidder_name, unit_price, extension, item_rank, is_low_item
        FROM bids
        WHERE contract_number = ?
        ORDER BY item_number, item_rank
        """, (contract_number,))
        
        item_bids = cursor.fetchall()
        
        conn.close()
        
        return {
            "contract": {
                "contract_number": contract['contract_number'],
                "letting_date": contract['letting_date'],
                "district": contract['district'],
                "county": contract['county'],
                "municipality": contract['municipality'],
                "project_number": contract['project_number'],
                "section": contract['section'],
                "num_bidders": contract['num_bidders'],
                "engineers_estimate": contract['engineers_estimate']
            },
            "bidders": [
                {
                    "bidder_id": row['bidder_number'],
                    "bidder_name": row['bidder_name'],
                    "rank": row['bidder_rank'],
                    "total_bid": row['total_bid_amount'],
                    "spread_pct": row['bid_spread_pct'],
                    "winner": row['is_winner'] == 'Y'
                } for row in bidders
            ],
            "items": [
                {
                    "item_number": row['item_number'],
                    "description": row['item_description'],
                    "quantity": row['quantity'],
                    "unit": row['unit'],
                    "bidder": row['bidder_name'],
                    "unit_price": row['unit_price'],
                    "extension": row['extension'],
                    "rank": row['item_rank'],
                    "is_low": row['is_low_item'] == 'Y'
                } for row in item_bids
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# PRICING ENDPOINTS
# ============================================================================

@router.get("/pricing/weighted-averages")
async def get_weighted_averages(
    skip: int = 0,
    limit: int = Query(100, le=500)
):
    """Get weighted average prices for all items"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        SELECT 
            item_number,
            item_description,
            unit,
            SUM(unit_price * quantity) / SUM(quantity) as weighted_avg_price,
            COUNT(*) as bid_count,
            SUM(quantity) as total_quantity,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price
        FROM bids
        GROUP BY item_number, item_description, unit
        ORDER BY bid_count DESC
        LIMIT ? OFFSET ?
        """, (limit, skip))
        
        results = cursor.fetchall()
        
        # Get total count
        cursor.execute("SELECT COUNT(DISTINCT item_number) FROM bids")
        total = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "items": [
                {
                    "pay_item": r['item_number'],
                    "description": r['item_description'],
                    "unit": r['unit'],
                    "weighted_avg_price": round(r['weighted_avg_price'], 2) if r['weighted_avg_price'] else None,
                    "total_bids": r['bid_count'],
                    "total_quantity": r['total_quantity'],
                    "min_price": r['min_price'],
                    "max_price": r['max_price']
                } for r in results
            ]
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pricing/by-county/{county}")
async def get_pricing_by_county(
    county: str,
    limit: int = Query(100, le=500)
):
    """Get average prices by county"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        SELECT 
            item_number,
            item_description,
            unit,
            AVG(unit_price) as avg_price,
            COUNT(*) as bid_count,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price
        FROM bids
        WHERE county LIKE ?
        GROUP BY item_number, item_description, unit
        HAVING COUNT(*) >= 3
        ORDER BY bid_count DESC
        LIMIT ?
        """, (f'%{county}%', limit))
        
        results = cursor.fetchall()
        conn.close()
        
        return {
            "county": county,
            "items": [
                {
                    "pay_item": r['item_number'],
                    "description": r['item_description'],
                    "unit": r['unit'],
                    "avg_price": round(r['avg_price'], 2),
                    "bid_count": r['bid_count'],
                    "min_price": r['min_price'],
                    "max_price": r['max_price']
                } for r in results
            ]
        }
        
    except Exception as e:
        conn.close()
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
        
        # Total contracts
        cursor.execute("SELECT COUNT(DISTINCT contract_number) FROM bids")
        stats['total_contracts'] = cursor.fetchone()[0]
        
        # Total bid rows (bidder-item combinations)
        cursor.execute("SELECT COUNT(*) FROM bids")
        stats['total_bid_rows'] = cursor.fetchone()[0]
        
        # Unique contractors
        cursor.execute("SELECT COUNT(DISTINCT bidder_name) FROM bids")
        stats['unique_contractors'] = cursor.fetchone()[0]
        
        # Unique pay items
        cursor.execute("SELECT COUNT(DISTINCT item_number) FROM bids")
        stats['unique_pay_items'] = cursor.fetchone()[0]
        
        # Counties covered
        cursor.execute("SELECT COUNT(DISTINCT county) FROM bids")
        stats['counties'] = cursor.fetchone()[0]
        
        # Date range
        cursor.execute("SELECT MIN(letting_date), MAX(letting_date) FROM bids")
        date_range = cursor.fetchone()
        stats['earliest_letting'] = date_range[0]
        stats['latest_letting'] = date_range[1]
        
        # Letting dates count
        cursor.execute("SELECT COUNT(DISTINCT letting_date) FROM bids")
        stats['letting_dates'] = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "platform_stats": stats,
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/contractors/top")
async def get_top_contractors(limit: int = Query(20, le=50)):
    """Get top contractors by bid volume"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        SELECT 
            bidder_name,
            COUNT(DISTINCT contract_number) as total_contracts,
            SUM(CASE WHEN is_winner = 'Y' THEN 1 ELSE 0 END) as contract_wins,
            COUNT(*) as total_item_bids,
            SUM(CASE WHEN is_low_item = 'Y' THEN 1 ELSE 0 END) as low_item_bids,
            AVG(total_bid_amount) as avg_bid
        FROM bids
        GROUP BY bidder_name
        HAVING COUNT(DISTINCT contract_number) >= 5
        ORDER BY total_contracts DESC
        LIMIT ?
        """, (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        return {
            "top_contractors": [
                {
                    "contractor": r['bidder_name'],
                    "total_contracts": r['total_contracts'],
                    "contract_wins": r['contract_wins'],
                    "win_rate": round(r['contract_wins'] / r['total_contracts'] * 100, 1) if r['total_contracts'] > 0 else 0,
                    "total_item_bids": r['total_item_bids'],
                    "low_item_bids": r['low_item_bids'],
                    "item_win_rate": round(r['low_item_bids'] / r['total_item_bids'] * 100, 1) if r['total_item_bids'] > 0 else 0,
                    "avg_bid": round(r['avg_bid'], 2) if r['avg_bid'] else None
                } for r in results
            ]
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/counties")
async def get_county_stats():
    """Get statistics by county"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        SELECT 
            county,
            COUNT(DISTINCT contract_number) as contracts,
            COUNT(DISTINCT bidder_name) as unique_bidders,
            COUNT(*) as total_bids
        FROM bids
        GROUP BY county
        ORDER BY contracts DESC
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        return {
            "counties": [
                {
                    "county": r['county'],
                    "contracts": r['contracts'],
                    "unique_bidders": r['unique_bidders'],
                    "total_bids": r['total_bids']
                } for r in results
            ]
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# BROWSE ENDPOINTS
# ============================================================================

@router.get("/contracts")
async def get_contracts(
    county: Optional[str] = None,
    year: Optional[str] = None,
    skip: int = 0,
    limit: int = Query(50, le=200)
):
    """Browse contracts with optional filters"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Build query with filters
        query = """
        SELECT DISTINCT
            contract_number, letting_date, district, county, 
            municipality, num_bidders
        FROM bids
        WHERE 1=1
        """
        params = []
        
        if county:
            query += " AND county LIKE ?"
            params.append(f'%{county}%')
        
        if year:
            query += " AND letting_date LIKE ?"
            params.append(f'%/{year}')
        
        query += " ORDER BY letting_date DESC LIMIT ? OFFSET ?"
        params.extend([limit, skip])
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Get total count
        count_query = "SELECT COUNT(DISTINCT contract_number) FROM bids WHERE 1=1"
        count_params = []
        if county:
            count_query += " AND county LIKE ?"
            count_params.append(f'%{county}%')
        if year:
            count_query += " AND letting_date LIKE ?"
            count_params.append(f'%/{year}')
        
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "contracts": [
                {
                    "contract_number": r['contract_number'],
                    "letting_date": r['letting_date'],
                    "district": r['district'],
                    "county": r['county'],
                    "municipality": r['municipality'],
                    "num_bidders": r['num_bidders']
                } for r in results
            ]
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/contractors")
async def get_contractors(
    skip: int = 0,
    limit: int = Query(50, le=200)
):
    """Browse all contractors"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        SELECT 
            bidder_name,
            bidder_number,
            COUNT(DISTINCT contract_number) as contracts,
            SUM(CASE WHEN is_winner = 'Y' THEN 1 ELSE 0 END) as wins
        FROM bids
        GROUP BY bidder_name, bidder_number
        ORDER BY contracts DESC
        LIMIT ? OFFSET ?
        """, (limit, skip))
        
        results = cursor.fetchall()
        
        cursor.execute("SELECT COUNT(DISTINCT bidder_name) FROM bids")
        total = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "contractors": [
                {
                    "bidder_name": r['bidder_name'],
                    "bidder_number": r['bidder_number'],
                    "contracts": r['contracts'],
                    "wins": r['wins'],
                    "win_rate": round(r['wins'] / r['contracts'] * 100, 1) if r['contracts'] > 0 else 0
                } for r in results
            ]
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# HEALTH CHECK
# ============================================================================

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM bids")
        count = cursor.fetchone()[0]
        conn.close()
        return {"status": "healthy", "bid_rows": count}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
