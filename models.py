from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class PriceData1m(Base):
    __tablename__ = 'price_data_1m'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    timestamp = Column(DateTime, index=True)
    price_open = Column(Float)
    price_close = Column(Float)
    price_high = Column(Float)
    price_low = Column(Float)
    volume = Column(Float)
    zap_id = Column(Integer)

class PriceData1h(Base):
    __tablename__ = 'price_data_1h'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    timestamp = Column(DateTime, index=True)
    price_open = Column(Float)
    price_close = Column(Float)
    price_high = Column(Float)
    price_low = Column(Float)
    volume = Column(Float)
    zap_id = Column(Integer)

class PriceData1d(Base):
    __tablename__ = 'price_data_1d'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    timestamp = Column(DateTime, index=True)
    price_open = Column(Float)
    price_close = Column(Float)
    price_high = Column(Float)
    price_low = Column(Float)
    volume = Column(Float)
    zap_id = Column(Integer)

# Create database
engine = create_engine('sqlite:///crypto_data.db')
Base.metadata.create_all(engine) 