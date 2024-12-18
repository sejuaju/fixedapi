from flask import Flask, jsonify, request
import json
import os
from collections import OrderedDict
from json import JSONEncoder
import boto3
from botocore.client import Config
import ijson

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, OrderedDict):
            return dict(obj)
        return super().default(obj)

app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

# Setup S3/R2 client
s3 = boto3.client('s3',
    endpoint_url = os.getenv('R2_ENDPOINT'),
    aws_access_key_id = os.getenv('R2_ACCESS_KEY'),
    aws_secret_access_key = os.getenv('R2_SECRET_KEY'),
    config = Config(signature_version='s3v4')
)

# Ubah konstanta path folder
MINUTE_DATA_FOLDER = '1m_json'  # Folder untuk data 1 menit
DEFAULT_DATA_FOLDER = '.'       # Folder untuk data 1h dan 1d

def get_file_path(symbol, timeframe):
    """Helper function untuk mendapatkan path file berdasarkan timeframe"""
    filename = f'{symbol.lower()}.json'
    if timeframe == '1m':
        return os.path.join(MINUTE_DATA_FOLDER, filename)
    return filename

def find_coin_file_by_id(id_type, id_value, timeframe='1h'):
    """Mencari file coin berdasarkan external_id atau zap_id"""
    folder = MINUTE_DATA_FOLDER if timeframe == '1m' else DEFAULT_DATA_FOLDER
    files = [f for f in os.listdir(folder) if f.endswith('.json') and len(f.split('.')[0]) <= 5]
    
    for f in files:
        with open(os.path.join(folder, f), 'r') as file:
            data = json.load(file)
            symbol = list(data.keys())[0]
            if data[symbol][id_type] == id_value:
                return os.path.join(folder, f)
    return None

@app.route('/api/v1/coins', methods=['GET'])
def get_available_coins():
    """Mendapatkan daftar coin yang tersedia"""
    try:
        files = [f for f in os.listdir('.') if f.endswith('.json') and len(f.split('.')[0]) <= 5]
        coins = []
        
        for f in files:
            with open(f, 'r') as file:
                data = json.load(file)
                symbol = list(data.keys())[0]
                coin_data = data[symbol]
                coins.append({
                    'symbol': symbol,
                    'name': coin_data['name'],
                    'zap_id': coin_data['zap_id'],
                    'external_id': coin_data['external_id'],
                    'source': 'coinapi',  # Sumber data
                    'source_id': coin_data.get('metadata', {}).get('exchange_listings', {}).get('best_pair', ''),
                    'volume': {
                        'last_24h': coin_data.get('metadata', {}).get('volume_stats', {}).get('last_24h', 0),
                        'last_7d': coin_data.get('metadata', {}).get('volume_stats', {}).get('last_7d', 0),
                        'last_30d': coin_data.get('metadata', {}).get('volume_stats', {}).get('last_30d', 0)
                    }
                })
        
        return app.response_class(
            response=json.dumps({
                'status': 'success',
                'data': coins,
                'total': len(coins)
            }, indent=2, cls=CustomJSONEncoder),
            status=200,
            mimetype='application/json'
        )
    
    except Exception as e:
        return app.response_class(
            response=json.dumps({'error': str(e)}, indent=2),
            status=500,
            mimetype='application/json'
        )

@app.route('/api/v1/history/<symbol>/<timeframe>', methods=['GET'])
def get_coin_history(symbol, timeframe):
    """Mengambil data historis coin berdasarkan symbol dan timeframe"""
    try:
        # Ambil parameter limit
        limit = request.args.get('limit', type=int)
        if not limit:
            limit = 100 if timeframe == '1m' else None

        key = f'{timeframe}/{symbol.lower()}.json'
        
        try:
            obj = s3.get_object(
                Bucket=os.getenv('R2_BUCKET'),
                Key=key
            )
            
            # Gunakan satu metode parsing saja
            if timeframe == '1m':
                # Gunakan ijson untuk streaming parse
                parser = ijson.parse(obj['Body'])
                data = {symbol.upper(): {'1m': [], 'zap_id': None, 'metadata': {}}}
                
                # Ambil hanya data yang diperlukan
                count = 0
                max_items = limit or 100
                
                for prefix, event, value in parser:
                    # Ambil metadata dulu
                    if prefix.endswith('.zap_id'):
                        data[symbol.upper()]['zap_id'] = value
                    elif prefix.endswith('.metadata'):
                        data[symbol.upper()]['metadata'] = value
                    # Kemudian ambil data historis
                    elif prefix.endswith('.1m.item'):
                        if count < max_items:
                            data[symbol.upper()]['1m'].append(value)
                            count += 1
            else:
                data = json.loads(obj['Body'].read())
        except s3.exceptions.NoSuchKey as e:
            print(f"File not found error: {str(e)}")
            return app.response_class(
                response=json.dumps({
                    'error': f'No data found for {symbol} with timeframe {timeframe}',
                    'details': str(e)
                }, indent=2),
                status=404,
                mimetype='application/json'
            )
        
        symbol_upper = symbol.upper()
        coin_data = data[symbol_upper]
        
        # Format data sesuai timeframe yang diminta
        formatted_data = []
        timeframe_key = {
            '1m': '1m',
            '1h': '1h',
            '1d': '1d'
        }[timeframe]
        
        # Ambil data dari timeframe yang sesuai
        data_list = coin_data[timeframe_key]
        
        # Terapkan limit jika ada
        if limit:
            data_list = data_list[-limit:]
        
        for item in data_list:
            formatted_data.append({
                "zap_id": coin_data['zap_id'],
                "timestamp": item['timestamp'],
                "price_open": item['price_open'],
                "price_close": item['price_close'],
                "price_high": item['price_high'],
                "price_low": item['price_low'],
                "volume": item['volume'],
                "market_cap": None,
                "source": "r2",
                "source_id": coin_data.get('metadata', {}).get('exchange_listings', {}).get('best_pair')
            })
        
        # Format response dengan metadata
        response_data = {
            "data": formatted_data,
            "metadata": {
                "symbol": symbol_upper,
                "timeframe": timeframe,
                "total_available": len(coin_data[timeframe_key]),
                "returned_count": len(formatted_data),
                "limit_applied": limit if limit else None
            }
        }
        
        return app.response_class(
            response=json.dumps(response_data, indent=2),
            status=200,
            mimetype='application/json'
        )
    
    except Exception as e:
        return app.response_class(
            response=json.dumps({'error': str(e)}, indent=2),
            status=500,
            mimetype='application/json'
        )

@app.route('/api/v1/history/external_id/<external_id>/<timeframe>', methods=['GET'])
def get_coin_by_external_id(external_id, timeframe):
    """Mengambil data historis coin berdasarkan external_id dan timeframe"""
    try:
        if timeframe not in ['1m', '1h', '1d']:
            return app.response_class(
                response=json.dumps({'error': 'Invalid timeframe. Use 1m, 1h or 1d'}, indent=2),
                status=400,
                mimetype='application/json'
            )

        filename = find_coin_file_by_id('external_id', external_id, timeframe)
        if not filename:
            return app.response_class(
                response=json.dumps({'error': f'No data found for external_id: {external_id} with timeframe {timeframe}'}, indent=2),
                status=404,
                mimetype='application/json'
            )
        
        with open(filename, 'r') as f:
            raw_data = json.load(f)
            symbol = list(raw_data.keys())[0]
            coin_data = raw_data[symbol]
            
            formatted_data = []
            timeframe_key = {
                '1m': '1m',
                '1h': '1h',
                '1d': '1d'
            }[timeframe]
            
            for item in coin_data[timeframe_key]:
                formatted_data.append({
                    "zap_id": coin_data['zap_id'],
                    "timestamp": item['timestamp'],
                    "price_open": item['price_open'],
                    "price_close": item['price_close'],
                    "price_high": item['price_high'],
                    "price_low": item['price_low'],
                    "volume": item['volume'],
                    "market_cap": None,
                    "source": "local",
                    "source_id": coin_data.get('metadata', {}).get('exchange_listings', {}).get('best_pair')
                })
                
            return app.response_class(
                response=json.dumps(formatted_data, indent=2),
                status=200,
                mimetype='application/json'
            )
    
    except Exception as e:
        return app.response_class(
            response=json.dumps({'error': str(e)}, indent=2),
            status=500,
            mimetype='application/json'
        )

@app.route('/api/v1/history/zap_id/<int:zap_id>/<timeframe>', methods=['GET'])
def get_coin_by_zap_id(zap_id, timeframe):
    """Mengambil data historis coin berdasarkan zap_id dan timeframe"""
    try:
        if timeframe not in ['1m', '1h', '1d']:
            return app.response_class(
                response=json.dumps({'error': 'Invalid timeframe. Use 1m, 1h or 1d'}, indent=2),
                status=400,
                mimetype='application/json'
            )

        filename = find_coin_file_by_id('zap_id', zap_id, timeframe)
        if not filename:
            return app.response_class(
                response=json.dumps({'error': f'No data found for zap_id: {zap_id} with timeframe {timeframe}'}, indent=2),
                status=404,
                mimetype='application/json'
            )
        
        with open(filename, 'r') as f:
            raw_data = json.load(f)
            symbol = list(raw_data.keys())[0]
            coin_data = raw_data[symbol]
            
            formatted_data = []
            timeframe_key = {
                '1m': '1m',
                '1h': '1h',
                '1d': '1d'
            }[timeframe]
            
            for item in coin_data[timeframe_key]:
                formatted_data.append({
                    "zap_id": coin_data['zap_id'],
                    "timestamp": item['timestamp'],
                    "price_open": item['price_open'],
                    "price_close": item['price_close'],
                    "price_high": item['price_high'],
                    "price_low": item['price_low'],
                    "volume": item['volume'],
                    "market_cap": None,
                    "source": "local",
                    "source_id": coin_data.get('metadata', {}).get('exchange_listings', {}).get('best_pair')
                })
                
            return app.response_class(
                response=json.dumps(formatted_data, indent=2),
                status=200,
                mimetype='application/json'
            )
    
    except Exception as e:
        return app.response_class(
            response=json.dumps({'error': str(e)}, indent=2),
            status=500,
            mimetype='application/json'
        )

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)