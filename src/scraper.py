import os
import json
import logging
import asyncio
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_ID = os.getenv('TG_API_ID')
API_HASH = os.getenv('TG_API_HASH')
SESSION_NAME = 'medical_scraper_session'

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logging.getLogger('').addHandler(console_handler)

# List of channels to scrape
# Note: 'CheMed123' is a placeholder, verify the actual username for "CheMed Telegram Channel"
CHANNELS = [
    'lobelia4cosmetics',
    'tikvahpharma',
    'CheMed123' 
]

async def scrape_channel(client, channel_name):
    """
    Scrapes messages and images from a single Telegram channel.
    """
    logging.info(f"Starting scrape for channel: {channel_name}")
    
    # Create directories for images
    channel_image_dir = f"data/raw/images/{channel_name}"
    os.makedirs(channel_image_dir, exist_ok=True)
    
    messages_data = []
    
    try:
        # Get the channel entity
        entity = await client.get_entity(channel_name)
        
        # Iterate over messages (limit set to 100 for initial testing)
        # Remove limit=100 or increase it for full scraping
        async for message in client.iter_messages(entity, limit=100):
            msg_data = {
                'message_id': message.id,
                'channel_name': channel_name,
                'date': message.date.isoformat(),
                'message_text': message.text,
                'views': getattr(message, 'views', 0),
                'forwards': getattr(message, 'forwards', 0),
                'has_media': False,
                'image_path': None
            }
            
            # Download image if present
            if message.photo:
                msg_data['has_media'] = True
                image_filename = f"{message.id}.jpg"
                image_path = os.path.join(channel_image_dir, image_filename)
                
                # Verify if we've already downloaded it to save bandwidth/time
                if not os.path.exists(image_path):
                    logging.info(f"Downloading image for message {message.id}")
                    await client.download_media(message.media, file=image_path)
                
                msg_data['image_path'] = image_path
            
            messages_data.append(msg_data)
            
        # Save metadata to JSON
        # File structure: data/raw/telegram_messages/YYYY-MM-DD/channel_name.json
        date_str = datetime.now().strftime('%Y-%m-%d')
        json_dir = f"data/raw/telegram_messages/{date_str}"
        os.makedirs(json_dir, exist_ok=True)
        json_path = os.path.join(json_dir, f"{channel_name}.json")
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(messages_data, f, ensure_ascii=False, indent=4)
            
        logging.info(f"Successfully scraped {channel_name}. Saved {len(messages_data)} messages to {json_path}")

    except Exception as e:
        logging.error(f"Error scraping channel {channel_name}: {str(e)}")

async def main():
    """
    Main entry point for the scraper.
    """
    if not API_ID or not API_HASH:
        logging.error("API_ID and API_HASH not found in .env file.")
        print("Error: Please set TG_API_ID and TG_API_HASH in your .env file.")
        return

    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        logging.info("Telegram client started.")
        for channel in CHANNELS:
            await scrape_channel(client, channel)
        logging.info("Scraping completed for all channels.")

if __name__ == '__main__':
    asyncio.run(main())
