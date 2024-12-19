from flask import Flask, jsonify, request
from models import engine, PriceData1m, PriceData1h, PriceData1d
from sqlalchemy.orm import Session
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

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

@app.route('/api/v1/history/<symbol>/<timeframe>', methods=['GET'])
def get_coin_history(symbol, timeframe):
    try:
        # Get limit from query params
        limit = request.args.get('limit', type=int, default=100)
        
        # Get appropriate model class
        PriceDataClass = get_price_data_class(timeframe)
        
        # Query from database
        with Session(engine) as session:
            results = session.query(PriceDataClass).filter(
                PriceDataClass.symbol == symbol.upper()
            ).order_by(
                PriceDataClass.timestamp.desc()
            ).limit(limit).all()
            
            # Format response
            formatted_data = [{
                "zap_id": item.zap_id,
                "timestamp": int(item.timestamp.timestamp()),
                "price_open": item.price_open,
                "price_close": item.price_close,
                "price_high": item.price_high,
                "price_low": item.price_low,
                "volume": item.volume,
                "market_cap": None,
                "source": "sqlite",
            } for item in results]
            
            # Response with metadata
            response_data = {
                "data": formatted_data,
                "metadata": {
                    "symbol": symbol.upper(),
                    "timeframe": timeframe,
                    "returned_count": len(formatted_data),
                    "limit_applied": limit
                }
            }
            
            return jsonify(response_data)
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)