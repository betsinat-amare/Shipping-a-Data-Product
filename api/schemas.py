from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

class Message(BaseModel):
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: Optional[str] = None
    views: int
    forwards: int
    
class ChannelActivity(BaseModel):
    date: str
    post_count: int
    
class VisualStats(BaseModel):
    image_category: str
    count: int
    avg_views: float

class TopProduct(BaseModel):
    product_name: str
    count: int
