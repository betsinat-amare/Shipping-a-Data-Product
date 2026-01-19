import os
import glob
import pandas as pd
from ultralytics import YOLO
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database Config
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def classify_image(detected_classes):
    """
    Classifies image based on detected objects.
    COCO Classes: person=0, bottle=39, cup=41, wine glass=40, bowl=45
    """
    has_person = 'person' in detected_classes
    # Define what constitutes a "product" in this context
    product_classes = {'bottle', 'cup', 'wine glass', 'bowl', 'vase', 'suitcase', 'handbag'}
    has_product = any(cls in product_classes for cls in detected_classes)
    
    if has_person and has_product:
        return 'promotional'
    elif has_product:
        return 'product_display'
    elif has_person:
        return 'lifestyle'
    else:
        return 'other'

def main():
    # 1. Load Model
    model = YOLO('yolov8n.pt')  # loads pretrained YOLOv8n model
    
    # 2. Find Images
    image_dir = 'data/raw/images'
    image_paths = glob.glob(os.path.join(image_dir, '**', '*.jpg'), recursive=True)
    
    if not image_paths:
        logging.warning("No images found to process.")
        return

    logging.info(f"Found {len(image_paths)} images. Starting detection...")
    
    results_list = []
    
    for img_path in image_paths:
        try:
            # Run inference
            results = model(img_path, verbose=False)
            result = results[0]
            
            # Extract detected classes and confidence
            detected_classes = []
            confidences = []
            
            for box in result.boxes:
                cls_id = int(box.cls[0])
                cls_name = model.names[cls_id]
                conf = float(box.conf[0])
                
                detected_classes.append(cls_name)
                confidences.append(conf)
            
            # Helper: get max confidence or 0
            max_conf = max(confidences) if confidences else 0.0
            
            # Determine category
            category = classify_image(set(detected_classes))
            
            # Extract message_id (filename without extension)
            # path: data/raw/images/channel_name/12345.jpg -> 12345
            message_id = os.path.splitext(os.path.basename(img_path))[0]
            
            # Sanity check if message_id is numeric
            if not message_id.isdigit():
                continue

            results_list.append({
                'message_id': int(message_id),
                'image_path': img_path,
                'detected_objects': detected_classes,  # List/Array
                'confidence_score': max_conf,
                'image_category': category
            })
            
        except Exception as e:
            logging.error(f"Error processing {img_path}: {e}")

    # 3. Save to CSV
    if not results_list:
        logging.info("No results generated.")
        return

    df = pd.DataFrame(results_list)
    output_dir = 'data/processed'
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, 'yolo_results.csv')
    df.to_csv(csv_path, index=False)
    logging.info(f"Results saved to {csv_path}")

    # 4. Load to Database
    # We need to serialize the list of objects for Postgres (e.g., to JSON string or Array)
    # SQLAlchemy/Pandas handle list->ARRAY mapping for Postgres if configured, 
    # but safer to ensure it's compatible. Let's rely on pandas default behavior 
    # which might need adjustment for List types. 
    # Simple approach: Convert list to comma-separated string for 'raw' storage 
    # or keep it as list and ensure table uses JSONB or ARRAY. 
    # 'to_sql' with sqlalchemy and lists can be tricky.
    # Let's convert to JSON string for safety in a raw table.
    
    df_db = df.copy()
    df_db['detected_objects'] = df_db['detected_objects'].apply(lambda x: str(x).replace("'", '"')) # Simple JSON-like string
    
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
            conn.commit()
            
        logging.info("Loading results to raw.image_detections...")
        df_db.to_sql('image_detections', engine, schema='raw', if_exists='replace', index=False)
        logging.info("Database load complete.")
        
    except Exception as e:
        logging.error(f"Database error: {e}")

if __name__ == "__main__":
    main()
