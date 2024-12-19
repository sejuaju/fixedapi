import json
import boto3
import ijson
from botocore.client import Config
import os
from models import engine, PriceData1m, PriceData1h, PriceData1d
from sqlalchemy.orm import Session
from datetime import datetime
from dotenv import load_dotenv
from dateutil.parser import parse

# Load environment variables
load_dotenv()

# Setup S3/R2 client
s3 = boto3.client('s3',
    endpoint_url = os.getenv('R2_ENDPOINT'),
    aws_access_key_id = os.getenv('R2_ACCESS_KEY'),
    aws_secret_access_key = os.getenv('R2_SECRET_KEY'),
    config = Config(signature_version='s3v4')
)

def get_price_data_class(timeframe):
    """Get the appropriate model class based on timeframe"""
    if timeframe == '1m':
        return PriceData1m
    elif timeframe == '1h':
        return PriceData1h
    elif timeframe == '1d':
        return PriceData1d
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")

def import_all_data():
    """Import all data from R2 bucket"""
    try:
        print("Starting import process...")
        
        # List semua file di R2
        response = s3.list_objects_v2(
            Bucket=os.getenv('R2_BUCKET')
        )
        
        # Process setiap folder (1m, 1h, 1d)
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.json'):
                # Extract timeframe dan symbol
                timeframe = obj['Key'].split('/')[0]  # '1m', '1h', '1d'
                symbol = obj['Key'].split('/')[1].split('.')[0].upper()  # 'BTC', 'ETH'
                
                print(f"Processing {symbol} {timeframe}...")
                
                # Get appropriate model class
                PriceDataClass = get_price_data_class(timeframe)
                
                # Get data
                obj_data = s3.get_object(
                    Bucket=os.getenv('R2_BUCKET'),
                    Key=obj['Key']
                )
                
                # Parse data
                parser = ijson.parse(obj_data['Body'])
                metadata = {}
                
                # Get metadata
                for prefix, event, value in parser:
                    if prefix.endswith('.zap_id'):
                        metadata['zap_id'] = value
                        metadata['symbol'] = prefix.split('.')[0]
                        break
                
                # Reset stream and parse price data
                obj_data = s3.get_object(
                    Bucket=os.getenv('R2_BUCKET'),
                    Key=obj['Key']
                )
                
                parser = ijson.items(obj_data['Body'], f'{metadata["symbol"]}.{timeframe}.item')
                
                # Import to SQLite
                count = 0
                with Session(engine) as session:
                    print(f"Starting database import for {timeframe}...")
                    
                    for item in parser:
                        try:
                            # Convert ISO timestamp to datetime
                            timestamp = parse(item['timestamp']).timestamp()
                            
                            price_data = PriceDataClass(
                                symbol=metadata['symbol'],
                                timestamp=datetime.fromtimestamp(timestamp),
                                price_open=float(item['price_open']),
                                price_close=float(item['price_close']),
                                price_high=float(item['price_high']),
                                price_low=float(item['price_low']),
                                volume=float(item['volume']),
                                zap_id=metadata['zap_id']
                            )
                            session.add(price_data)
                            count += 1
                            if count % 1000 == 0:
                                print(f"Imported {count} records...")
                                session.commit()
                        except Exception as e:
                            print(f"Error processing record: {str(e)}")
                            print(f"Record data: {item}")
                            continue
                    
                    session.commit()
                    print(f"Import completed for {timeframe}. Total records: {count}")
                    
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e

if __name__ == '__main__':
    import_all_data() 