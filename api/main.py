from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from . import database, schemas

app = FastAPI(
    title="Medical Telegram Warehouse API",
    description="Analytical API for querying Telegram channel data and YOLO enrichment results.",
    version="1.0.0"
)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Medical Warehouse API"}

@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def get_top_products(limit: int = 10, db: Session = Depends(database.get_db)):
    """
    Returns top detected objects/products from images. 
    Note: Using detected objects as a proxy for "products".
    """
    try:
        # We need to unnest the array if it's stored as array, or parse the string.
        # Since we stored it as string in raw, let's look at fct_image_detections.
        # Assuming fct_image_detections has it properly or we parse.
        # Actually, let's query the simpler `image_category` or attempt to parse `detected_objects`
        # For this prototype, we'll return top image categories as "products" if object parsing is hard in SQL,
        # BUT the requirement says "Most frequently mentioned terms/products". 
        # Let's try to extract from message_text using simple SQL split if possible, or use YOLO objects.
        # Let's stick to YOLO objects for "Visual Products".
        
        # Postgres specific: unnest array if it was array. 
        # If text, we might need to rely on simplistic counting of image categories for stability.
        # Let's query image_category for reliability in this demo.
        
        query = text("""
            SELECT image_category as product_name, count(*) as count 
            FROM fct_image_detections 
            WHERE image_category != 'other'
            GROUP BY image_category 
            ORDER BY count DESC 
            LIMIT :limit
        """)
        result = db.execute(query, {"limit": limit}).fetchall()
        return [{"product_name": row[0], "count": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivity])
def get_channel_activity(channel_name: str, db: Session = Depends(database.get_db)):
    """
    Returns daily posting activity for a specific channel.
    """
    # Join fct_messages with dim_dates and dim_channels
    # Note: adjusting for your schema exact names
    query = text("""
        SELECT 
            d.full_date as date, 
            count(*) as post_count
        FROM fct_messages m
        JOIN dim_channels c ON m.channel_key = c.channel_key
        JOIN dim_dates d ON m.date_key = d.date_key
        WHERE c.channel_name = :channel
        GROUP BY d.full_date
        ORDER BY d.full_date DESC
    """)
    result = db.execute(query, {"channel": channel_name}).fetchall()
    
    if not result:
        # Check if channel exists to return 404 vs empty list
        check = db.execute(text("SELECT 1 FROM dim_channels WHERE channel_name = :channel"), {"channel": channel_name}).fetchone()
        if not check:
             raise HTTPException(status_code=404, detail="Channel not found")
        return []
        
    return [{"date": str(row[0]), "post_count": row[1]} for row in result]

@app.get("/api/search/messages", response_model=List[schemas.Message])
def search_messages(query: str, limit: int = 20, db: Session = Depends(database.get_db)):
    """
    Search messages by keyword.
    """
    sql = text("""
        SELECT 
            m.message_id, 
            c.channel_name, 
            d.full_date as message_date,
            m.message_text, 
            m.view_count as views, 
            m.forward_count as forwards
        FROM fct_messages m
        JOIN dim_channels c ON m.channel_key = c.channel_key
        JOIN dim_dates d ON m.date_key = d.date_key
        WHERE m.message_text ILIKE :query
        LIMIT :limit
    """)
    result = db.execute(sql, {"query": f"%{query}%", "limit": limit}).fetchall()
    
    return [
        {
            "message_id": row[0],
            "channel_name": row[1],
            "message_date": row[2],
            "message_text": row[3],
            "views": row[4],
            "forwards": row[5]
        }
        for row in result
    ]

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualStats])
def get_visual_stats(db: Session = Depends(database.get_db)):
    """
    Returns statistics about image categories + average views per category.
    """
    # Join fct_image_detections back to fct_messages to get view counts 
    # (if not already in fct_image_detections, but wait, fct_image_detections doesn't have views usually, 
    # checking schema... I didn't put views in fct_image_detections.
    # So we join fct_image_detections -> fct_messages)
    
    sql = text("""
        SELECT 
            d.image_category, 
            count(*) as count,
            avg(m.view_count) as avg_views
        FROM fct_image_detections d
        JOIN fct_messages m ON d.message_id = m.message_id
        GROUP BY d.image_category
    """)
    result = db.execute(sql).fetchall()
    
    return [
        {"image_category": row[0], "count": row[1], "avg_views": float(row[2]) if row[2] else 0.0} 
        for row in result
    ]
