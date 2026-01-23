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
# STATS / ANALYTICS ENDPOINTS
# ============================================================================

@router.get("/stats")
async def get_stats():
    """Get overall database statistics"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM bids")
    total_bids = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT contract_number) FROM bids")
    total_contracts = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT bidder_name) FROM bids")
    total_contractors = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT item_number) FROM bids")
    total_items = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "total_bids": total_bids,
        "total_contracts": total_contracts,
        "total_contractors": total_contractors,
        "total_items": total_items
    }


@router.get("/analytics/summary")
async def get_analytics_summary():
    """Get comprehensive analytics summary for dashboard"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Total bid rows
    cursor.execute("SELECT COUNT(*) FROM bids")
    total_bid_rows = cursor.fetchone()[0]
    
    # Unique contracts
    cursor.execute("SELECT COUNT(DISTINCT contract_number) FROM bids")
    unique_contracts = cursor.fetchone()[0]
    
    # Unique contractors
    cursor.execute("SELECT COUNT(DISTINCT bidder_name) FROM bids")
    unique_contractors = cursor.fetchone()[0]
    
    # Unique items
    cursor.execute("SELECT COUNT(DISTINCT item_number) FROM bids")
    unique_items = cursor.fetchone()[0]
    
    # Unique counties
    cursor.execute("SELECT COUNT(DISTINCT county) FROM bids WHERE county IS NOT NULL AND county != ''")
    unique_counties = cursor.fetchone()[0]
    
    # Year range
    cursor.execute("""
        SELECT 
            MIN(CAST(substr(letting_date, length(letting_date)-3) AS INTEGER)) as min_year,
            MAX(CAST(substr(letting_date, length(letting_date)-3) AS INTEGER)) as max_year
        FROM bids
        WHERE letting_date IS NOT NULL
    """)
    year_row = cursor.fetchone()
    
    conn.close()
    
    return {
        "total_bid_rows": total_bid_rows,
        "unique_contracts": unique_contracts,
        "unique_contractors": unique_contractors,
        "unique_items": unique_items,
        "unique_counties": unique_counties,
        "year_range": {
            "min": year_row['min_year'] if year_row else None,
            "max": year_row['max_year'] if year_row else None
        }
    }


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

# ============================================================================
# PAY ITEM SEARCH
# ============================================================================

@router.get("/search/pay-item/{item_number}")
async def search_pay_item(
    item_number: str,
    county: Optional[str] = None,
    district: Optional[str] = None,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    limit: int = Query(default=1000, le=5000)
):
    """
    Search for a pay item and get pricing history from ALL bidders.
    Returns unit prices, quantities, and yearly trends with WEIGHTED averages.
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
            district,
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
    
    if district:
        query += " AND district LIKE ?"
        params.append(f"%{district}%")
    
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
    
    # Get yearly statistics with WEIGHTED averages
    stats_query = """
        SELECT 
            substr(letting_date, length(letting_date)-3) as year,
            COUNT(*) as bid_count,
            SUM(extension) / NULLIF(SUM(quantity), 0) as weighted_avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price,
            SUM(quantity) as total_quantity,
            SUM(extension) as total_value
        FROM bids
        WHERE item_number LIKE ?
        AND unit_price > 0
        AND quantity > 0
    """
    stats_params = [f"%{item_number}%"]
    
    if county:
        stats_query += " AND county LIKE ?"
        stats_params.append(f"%{county}%")
    
    if district:
        stats_query += " AND district LIKE ?"
        stats_params.append(f"%{district}%")
    
    stats_query += " GROUP BY substr(letting_date, length(letting_date)-3) ORDER BY year"
    
    cursor.execute(stats_query, stats_params)
    yearly_stats = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return {
        "item_number": item_number,
        "filters": {"county": county, "district": district, "year_start": year_start, "year_end": year_end},
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
            district,
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
    county: Optional[str] = None,
    district: Optional[str] = None,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None
):
    """
    Search contractor bidding history.
    Returns contract-level summary with win rates.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    # Build WHERE clause for filters
    where_clause = "WHERE bidder_name LIKE ?"
    params = [f"%{name}%"]
    
    if county:
        where_clause += " AND county LIKE ?"
        params.append(f"%{county}%")
    
    if district:
        where_clause += " AND district LIKE ?"
        params.append(f"%{district}%")
    
    if year_start:
        where_clause += " AND CAST(substr(letting_date, length(letting_date)-3) AS INTEGER) >= ?"
        params.append(year_start)
    
    if year_end:
        where_clause += " AND CAST(substr(letting_date, length(letting_date)-3) AS INTEGER) <= ?"
        params.append(year_end)
    
    # Get contract-level summary (one row per contract)
    contracts_query = f"""
        SELECT 
            contract_number,
            letting_date,
            county,
            district,
            bidder_name,
            bidder_rank,
            total_bid_amount,
            bid_spread_pct,
            is_winner,
            COUNT(*) as item_count
        FROM bids
        {where_clause}
        GROUP BY contract_number, bidder_name
        ORDER BY letting_date DESC
    """
    
    cursor.execute(contracts_query, params)
    contracts = [dict(row) for row in cursor.fetchall()]
    
    # Get win statistics
    stats_query = f"""
        SELECT 
            bidder_name,
            COUNT(DISTINCT contract_number) as contracts_bid,
            COUNT(DISTINCT CASE WHEN is_winner = 'Y' THEN contract_number END) as contracts_won,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN is_winner = 'Y' THEN contract_number END) / 
                COUNT(DISTINCT contract_number), 1) as win_rate,
            ROUND(AVG(bidder_rank), 1) as avg_rank
        FROM bids
        {where_clause}
        GROUP BY bidder_name
    """
    
    cursor.execute(stats_query, params)
    stats_rows = cursor.fetchall()
    
    # Get total won value separately
    won_query = f"""
        SELECT SUM(total_bid_amount) as total_won_value
        FROM (
            SELECT DISTINCT contract_number, total_bid_amount
            FROM bids
            {where_clause} AND is_winner = 'Y'
        )
    """
    
    cursor.execute(won_query, params)
    won_value_row = cursor.fetchone()
    total_won_value = won_value_row['total_won_value'] if won_value_row and won_value_row['total_won_value'] else 0
    
    stats = []
    for row in stats_rows:
        stat = dict(row)
        stat['total_won_value'] = total_won_value
        stats.append(stat)
    
    conn.close()
    
    return {
        "search_term": name,
        "filters": {"county": county, "district": district, "year_start": year_start, "year_end": year_end},
        "contractor_stats": stats,
        "contract_count": len(contracts),
        "contracts": contracts
    }

# ============================================================================
# CONTRACT SEARCH
# ============================================================================

@router.get("/search/contract/{contract_number}")
async def search_contract(contract_number: str):
    """
    Get all bids for a specific contract.
    Returns data organized for item-by-item comparison across bidders.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT *
        FROM bids
        WHERE contract_number LIKE ?
        ORDER BY item_number, bidder_rank
    """, [f"%{contract_number}%"])
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    bids = [dict(row) for row in rows]
    
    # Organize by item for comparison view
    items_comparison = {}
    bidders_info = {}
    
    for bid in bids:
        item_num = bid['item_number']
        bidder = bid['bidder_name']
        
        # Track bidder info
        if bidder not in bidders_info:
            bidders_info[bidder] = {
                'rank': bid['bidder_rank'],
                'total_bid': bid['total_bid_amount'],
                'is_winner': bid['is_winner']
            }
        
        # Organize items
        if item_num not in items_comparison:
            items_comparison[item_num] = {
                'item_number': item_num,
                'item_description': bid['item_description'],
                'quantity': bid['quantity'],
                'unit': bid['unit'],
                'engineers_estimate': bid.get('engineers_est_unit_price'),
                'bidder_prices': {}
            }
        
        items_comparison[item_num]['bidder_prices'][bidder] = {
            'unit_price': bid['unit_price'],
            'extension': bid['extension'],
            'is_winner': bid['is_winner']
        }
    
    # Sort bidders by rank
    sorted_bidders = sorted(bidders_info.items(), key=lambda x: x[1]['rank'])
    
    return {
        "contract_number": contract_number,
        "result_count": len(bids),
        "bidders": [{"name": name, **info} for name, info in sorted_bidders],
        "items_comparison": list(items_comparison.values()),
        "bids": bids  # Keep raw bids for backward compatibility
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
    Includes WEIGHTED average prices, price ranges, and bid counts.
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
            SUM(extension) / NULLIF(SUM(quantity), 0) as weighted_avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price,
            SUM(quantity) as total_quantity
        FROM bids
        WHERE unit_price > 0 AND quantity > 0
        GROUP BY item_number, item_description, unit
        HAVING COUNT(*) >= ?
        ORDER BY bid_count DESC
        LIMIT ?
    """, [min_occurrences, limit])
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "min_occurrences": min_occurrences,
        "result_count": len(rows),
        "items": [dict(row) for row in rows]
    }


@router.get("/pricing/county-comparison/{item_number}")
async def get_county_comparison(item_number: str):
    """
    Compare pricing for an item across counties with WEIGHTED averages.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            county,
            COUNT(*) as bid_count,
            SUM(extension) / NULLIF(SUM(quantity), 0) as weighted_avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price
        FROM bids
        WHERE item_number LIKE ?
        AND unit_price > 0
        AND quantity > 0
        GROUP BY county
        HAVING COUNT(*) >= 3
        ORDER BY weighted_avg_price DESC
    """, [f"%{item_number}%"])
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "item_number": item_number,
        "counties": [dict(row) for row in rows]
    }


@router.get("/pricing/district-comparison/{item_number}")
async def get_district_comparison(item_number: str):
    """
    Compare pricing for an item across districts with WEIGHTED averages.
    """
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            district,
            COUNT(*) as bid_count,
            SUM(extension) / NULLIF(SUM(quantity), 0) as weighted_avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price,
            SUM(quantity) as total_quantity
        FROM bids
        WHERE item_number LIKE ?
        AND unit_price > 0
        AND quantity > 0
        AND district IS NOT NULL
        GROUP BY district
        HAVING COUNT(*) >= 3
        ORDER BY weighted_avg_price DESC
    """, [f"%{item_number}%"])
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "item_number": item_number,
        "districts": [dict(row) for row in rows]
    }

# ============================================================================
# BROWSE / LIST ENDPOINTS
# ============================================================================

@router.get("/browse/items")
async def browse_items(
    search: Optional[str] = None,
    limit: int = Query(default=100, le=500)
):
    """Browse all pay items with optional search - uses WEIGHTED averages"""
    conn = get_db()
    cursor = conn.cursor()
    
    if search:
        cursor.execute("""
            SELECT 
                item_number,
                item_description,
                unit,
                COUNT(*) as bid_count,
                COUNT(DISTINCT contract_number) as contract_count,
                ROUND(SUM(extension) / NULLIF(SUM(quantity), 0), 2) as avg_price
            FROM bids
            WHERE (item_number LIKE ? OR item_description LIKE ?)
            AND unit_price > 0
            AND quantity > 0
            GROUP BY item_number, item_description, unit
            ORDER BY bid_count DESC
            LIMIT ?
        """, [f"%{search}%", f"%{search}%", limit])
    else:
        cursor.execute("""
            SELECT 
                item_number,
                item_description,
                unit,
                COUNT(*) as bid_count,
                COUNT(DISTINCT contract_number) as contract_count,
                ROUND(SUM(extension) / NULLIF(SUM(quantity), 0), 2) as avg_price
            FROM bids
            WHERE unit_price > 0
            AND quantity > 0
            GROUP BY item_number, item_description, unit
            ORDER BY bid_count DESC
            LIMIT ?
        """, [limit])
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "search": search,
        "result_count": len(rows),
        "items": [dict(row) for row in rows]
    }


@router.get("/browse/contractors")
async def browse_contractors(
    search: Optional[str] = None,
    limit: int = Query(default=100, le=500)
):
    """Browse all contractors with optional search"""
    conn = get_db()
    cursor = conn.cursor()
    
    base_query = """
        SELECT 
            bidder_name,
            COUNT(DISTINCT contract_number) as contracts_bid,
            COUNT(DISTINCT CASE WHEN is_winner = 'Y' THEN contract_number END) as contracts_won,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN is_winner = 'Y' THEN contract_number END) / 
                COUNT(DISTINCT contract_number), 1) as win_rate
        FROM bids
    """
    
    if search:
        base_query += " WHERE bidder_name LIKE ?"
        base_query += " GROUP BY bidder_name ORDER BY contracts_bid DESC LIMIT ?"
        cursor.execute(base_query, [f"%{search}%", limit])
    else:
        base_query += " GROUP BY bidder_name ORDER BY contracts_bid DESC LIMIT ?"
        cursor.execute(base_query, [limit])
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "search": search,
        "result_count": len(rows),
        "contractors": [dict(row) for row in rows]
    }


@router.get("/browse/contracts")
async def browse_contracts(
    county: Optional[str] = None,
    district: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = Query(default=100, le=500)
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
    
    if district:
        query += " AND district LIKE ?"
        params.append(f"%{district}%")
    
    if year:
        query += " AND CAST(substr(letting_date, length(letting_date)-3) AS INTEGER) = ?"
        params.append(year)
    
    query += " ORDER BY letting_date DESC LIMIT ?"
    params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "filters": {"county": county, "district": district, "year": year},
        "result_count": len(rows),
        "contracts": [dict(row) for row in rows]
    }


@router.get("/browse/districts")
async def browse_districts():
    """Get list of all districts"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT district, COUNT(DISTINCT contract_number) as contract_count
        FROM bids
        WHERE district IS NOT NULL AND district != ''
        GROUP BY district
        ORDER BY district
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "districts": [dict(row) for row in rows]
    }


@router.get("/browse/counties")
async def browse_counties():
    """Get list of all counties"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT county, COUNT(DISTINCT contract_number) as contract_count
        FROM bids
        WHERE county IS NOT NULL AND county != ''
        GROUP BY county
        ORDER BY county
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    return {
        "counties": [dict(row) for row in rows]
    }
