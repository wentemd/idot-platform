"""
IDOT Bid Intelligence Platform - API Routes
Routes for the flat bids table schema
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import sqlite3
import os

router = APIRouter()

def get_db():
    """Get database connection"""
    db_path = os.getenv("DATABASE_PATH", "/app/data/idot_intelligence.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# ============================================================================
# PAY ITEM SEARCH
# ============================================================================

@router.get("/search/pay-item/{item_number}")
async def search_pay_item(
    item_number: str,
    county: Optional[str] = None,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    limit: int = Query(default=1000, le=5000)
):
    """
    Search for a pay item and get pricing history from ALL bidders.
    Returns unit prices, quantities, and yearly trends.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    # Build query
    query = """
        SELECT 
            item_number,
            item_description,
            contract_number,
            letting_date,
            substr(letting_date, length(letting_date)-3) as letting_year,
            county,
            bidder_name,
            bidder_rank,
            is_winner,
            quantity,
            unit,
            unit_price,
            extension,
            engineers_est_unit_price,
            is_low_item,
            item_rank
        FROM bids
        WHERE item_number LIKE ?
    """
    params = [f"%{item_number}%"]
    
    if county:
        query += " AND county LIKE ?"
        params.append(f"%{county}%")
    
    if year_start:
        query += " AND CAST(substr(letting_date, length(letting_date)-3) AS INTEGER) >= ?"
        params.append(year_start)
    
    if year_end:
        query += " AND CAST(substr(letting_date, length(letting_date)-3) AS INTEGER) <= ?"
        params.append(year_end)
    
    query += " ORDER BY letting_date DESC, contract_number, bidder_rank LIMIT ?"
    params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    # Get yearly statistics
    stats_query = """
        SELECT 
            substr(letting_date, length(letting_date)-3) as year,
            COUNT(*) as bid_count,
            AVG(unit_price) as avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price,
            AVG(quantity) as avg_quantity
        FROM bids
        WHERE item_number LIKE ?
        AND unit_price > 0
    """
    stats_params = [f"%{item_number}%"]
    
    if county:
        stats_query += " AND county LIKE ?"
        stats_params.append(f"%{county}%")
    
    stats_query += " GROUP BY substr(letting_date, length(letting_date)-3) ORDER BY year"
    
    cursor.execute(stats_query, stats_params)
    yearly_stats = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return {
        "item_number": item_number,
        "filters": {"county": county, "year_start": year_start, "year_end": year_end},
        "result_count": len(rows),
        "yearly_trends": yearly_stats,
        "bids": [dict(row) for row in rows]
    }


@router.get("/search/pay-item-exact/{item_number}")
async def search_pay_item_exact(
    item_number: str,
    limit: int = Query(default=500, le=2000)
):
    """Exact match search for a specific pay item code"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            item_number,
            item_description,
            contract_number,
            letting_date,
            county,
            bidder_name,
            bidder_rank,
            is_winner,
            quantity,
            unit,
            unit_price,
            extension
        FROM bids
        WHERE item_number = ?
        ORDER BY letting_date DESC
        LIMIT ?
    """, [item_number, limit])
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "item_number": item_number,
        "result_count": len(rows),
        "bids": [dict(row) for row in rows]
    }

# ============================================================================
# CONTRACTOR SEARCH
# ============================================================================

@router.get("/search/contractor/{name}")
async def search_contractor(
    name: str,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    limit: int = Query(default=1000, le=5000)
):
    """
    Search contractor bidding history.
    Returns all bids from a contractor with win rates and item pricing.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    # Get contractor bids
    query = """
        SELECT 
            contract_number,
            letting_date,
            substr(letting_date, length(letting_date)-3) as letting_year,
            county,
            bidder_name,
            bidder_rank,
            total_bid_amount,
            bid_spread_pct,
            is_winner,
            item_number,
            item_description,
            quantity,
            unit,
            unit_price,
            extension
        FROM bids
        WHERE bidder_name LIKE ?
    """
    params = [f"%{name}%"]
    
    if year_start:
        query += " AND CAST(substr(letting_date, length(letting_date)-3) AS INTEGER) >= ?"
        params.append(year_start)
    
    if year_end:
        query += " AND CAST(substr(letting_date, length(letting_date)-3) AS INTEGER) <= ?"
        params.append(year_end)
    
    query += " ORDER BY letting_date DESC, contract_number LIMIT ?"
    params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    # Get win statistics
    cursor.execute("""
        SELECT 
            bidder_name,
            COUNT(DISTINCT contract_number) as contracts_bid,
            SUM(CASE WHEN is_winner = 'Yes' THEN 1 ELSE 0 END) / 
                CAST(COUNT(DISTINCT contract_number || bidder_name) AS FLOAT) as win_rate,
            AVG(bidder_rank) as avg_rank,
            SUM(CASE WHEN is_winner = 'Yes' THEN total_bid_amount ELSE 0 END) as total_won_value
        FROM (
            SELECT DISTINCT contract_number, bidder_name, is_winner, bidder_rank, total_bid_amount
            FROM bids
            WHERE bidder_name LIKE ?
        )
        GROUP BY bidder_name
    """, [f"%{name}%"])
    
    stats_rows = cursor.fetchall()
    stats = [dict(row) for row in stats_rows]
    
    conn.close()
    
    return {
        "search_term": name,
        "contractor_stats": stats,
        "result_count": len(rows),
        "bids": [dict(row) for row in rows]
    }

# ============================================================================
# CONTRACT SEARCH
# ============================================================================

@router.get("/search/contract/{contract_number}")
async def search_contract(contract_number: str):
    """
    Get all bids for a specific contract.
    Shows every bidder's prices for every item.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT *
        FROM bids
        WHERE contract_number LIKE ?
        ORDER BY bidder_rank, item_number
    """, [f"%{contract_number}%"])
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    return {
        "contract_number": contract_number,
        "result_count": len(rows),
        "bids": [dict(row) for row in rows]
    }

# ============================================================================
# PRICING ANALYTICS
# ============================================================================

@router.get("/pricing/item-summary")
async def get_item_pricing_summary(
    min_occurrences: int = Query(default=10, description="Minimum bid count to include"),
    limit: int = Query(default=100, le=500)
):
    """
    Get pricing summary for all items with sufficient data.
    Includes average prices, price ranges, and bid counts.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            item_number,
            item_description,
            unit,
            COUNT(*) as bid_count,
            COUNT(DISTINCT contract_number) as contract_count,
            AVG(unit_price) as avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price,
            AVG(quantity) as avg_quantity
        FROM bids
        WHERE unit_price > 0
        GROUP BY item_number, item_description, unit
        HAVING COUNT(*) >= ?
        ORDER BY bid_count DESC
        LIMIT ?
    """, [min_occurrences, limit])
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "min_occurrences": min_occurrences,
        "item_count": len(rows),
        "items": [dict(row) for row in rows]
    }


@router.get("/pricing/by-county/{county}")
async def get_county_pricing(
    county: str,
    item_number: Optional[str] = None,
    limit: int = Query(default=100, le=500)
):
    """Get pricing data filtered by county"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            item_number,
            item_description,
            unit,
            COUNT(*) as bid_count,
            AVG(unit_price) as avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price
        FROM bids
        WHERE county LIKE ?
        AND unit_price > 0
    """
    params = [f"%{county}%"]
    
    if item_number:
        query += " AND item_number LIKE ?"
        params.append(f"%{item_number}%")
    
    query += """
        GROUP BY item_number, item_description, unit
        HAVING COUNT(*) >= 3
        ORDER BY bid_count DESC
        LIMIT ?
    """
    params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "county": county,
        "item_filter": item_number,
        "item_count": len(rows),
        "items": [dict(row) for row in rows]
    }

# ============================================================================
# ANALYTICS ENDPOINTS
# ============================================================================

@router.get("/analytics/summary")
async def get_platform_summary():
    """Get overall platform statistics"""
    conn = get_db()
    cursor = conn.cursor()
    
    stats = {}
    
    # Total rows
    cursor.execute("SELECT COUNT(*) FROM bids")
    stats["total_bid_rows"] = cursor.fetchone()[0]
    
    # Unique contracts
    cursor.execute("SELECT COUNT(DISTINCT contract_number) FROM bids")
    stats["unique_contracts"] = cursor.fetchone()[0]
    
    # Unique contractors
    cursor.execute("SELECT COUNT(DISTINCT bidder_name) FROM bids")
    stats["unique_contractors"] = cursor.fetchone()[0]
    
    # Unique items
    cursor.execute("SELECT COUNT(DISTINCT item_number) FROM bids")
    stats["unique_items"] = cursor.fetchone()[0]
    
    # Year range
    cursor.execute("""
        SELECT 
            MIN(substr(letting_date, length(letting_date)-3)) as min_year,
            MAX(substr(letting_date, length(letting_date)-3)) as max_year
        FROM bids
    """)
    row = cursor.fetchone()
    stats["year_range"] = {"min": row[0], "max": row[1]}
    
    # Counties
    cursor.execute("SELECT COUNT(DISTINCT county) FROM bids WHERE county IS NOT NULL AND county != ''")
    stats["unique_counties"] = cursor.fetchone()[0]
    
    # Bids by year
    cursor.execute("""
        SELECT 
            substr(letting_date, length(letting_date)-3) as year,
            COUNT(*) as row_count,
            COUNT(DISTINCT contract_number) as contracts
        FROM bids
        GROUP BY substr(letting_date, length(letting_date)-3)
        ORDER BY year
    """)
    stats["by_year"] = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return stats


@router.get("/analytics/contractors/top")
async def get_top_contractors(
    limit: int = Query(default=25, le=100),
    year: Optional[int] = None
):
    """Get top contractors by contracts won"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            bidder_name,
            COUNT(DISTINCT contract_number) as contracts_bid,
            SUM(CASE WHEN is_winner = 'Yes' THEN 1 ELSE 0 END) as contracts_won,
            ROUND(AVG(bidder_rank), 2) as avg_rank
        FROM (
            SELECT DISTINCT contract_number, bidder_name, is_winner, bidder_rank
            FROM bids
    """
    params = []
    
    if year:
        query += " WHERE substr(letting_date, length(letting_date)-3) = ?"
        params.append(str(year))
    
    query += """
        )
        GROUP BY bidder_name
        HAVING contracts_bid >= 3
        ORDER BY contracts_won DESC, contracts_bid DESC
        LIMIT ?
    """
    params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "year_filter": year,
        "contractors": [dict(row) for row in rows]
    }


@router.get("/analytics/counties")
async def get_county_stats():
    """Get bidding statistics by county"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            county,
            COUNT(DISTINCT contract_number) as contracts,
            COUNT(DISTINCT bidder_name) as contractors,
            COUNT(*) as total_bid_rows
        FROM bids
        WHERE county IS NOT NULL AND county != ''
        GROUP BY county
        ORDER BY contracts DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    return {"counties": [dict(row) for row in rows]}

# ============================================================================
# BROWSE ENDPOINTS
# ============================================================================

@router.get("/contracts")
async def list_contracts(
    county: Optional[str] = None,
    year: Optional[int] = None,
    bidder: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0)
):
    """Browse contracts with optional filters"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = """
        SELECT DISTINCT
            contract_number,
            letting_date,
            county,
            district,
            project_type,
            num_bidders,
            engineers_estimate,
            awarded
        FROM bids
        WHERE 1=1
    """
    params = []
    
    if county:
        query += " AND county LIKE ?"
        params.append(f"%{county}%")
    
    if year:
        query += " AND substr(letting_date, length(letting_date)-3) = ?"
        params.append(str(year))
    
    if bidder:
        query += " AND contract_number IN (SELECT DISTINCT contract_number FROM bids WHERE bidder_name LIKE ?)"
        params.append(f"%{bidder}%")
    
    query += " ORDER BY letting_date DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "filters": {"county": county, "year": year, "bidder": bidder},
        "limit": limit,
        "offset": offset,
        "contracts": [dict(row) for row in rows]
    }


@router.get("/contractors")
async def list_contractors(
    search: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0)
):
    """Browse all contractors"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            bidder_name,
            COUNT(DISTINCT contract_number) as contracts_bid,
            MIN(letting_date) as first_bid,
            MAX(letting_date) as last_bid
        FROM bids
    """
    params = []
    
    if search:
        query += " WHERE bidder_name LIKE ?"
        params.append(f"%{search}%")
    
    query += """
        GROUP BY bidder_name
        ORDER BY contracts_bid DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "search": search,
        "limit": limit,
        "offset": offset,
        "contractors": [dict(row) for row in rows]
    }


@router.get("/items")
async def list_items(
    search: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0)
):
    """Browse all pay items"""
    conn = get_db()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            item_number,
            item_description,
            unit,
            COUNT(*) as bid_count,
            COUNT(DISTINCT contract_number) as contract_count,
            ROUND(AVG(unit_price), 2) as avg_unit_price
        FROM bids
        WHERE unit_price > 0
    """
    params = []
    
    if search:
        query += " AND (item_number LIKE ? OR item_description LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    
    query += """
        GROUP BY item_number, item_description, unit
        ORDER BY bid_count DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "search": search,
        "limit": limit,
        "offset": offset,
        "items": [dict(row) for row in rows]
    }

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
