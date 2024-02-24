# This example uses SQLAlchemy for a relational database
from sqlalchemy import Column, Integer, String, Float, Date, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class OptionData(Base):
    __tablename__ = 'option_data'
    
    id = Column(Integer, primary_key=True)
    strike_price = Column(Float)
    expiration_date = Column(Date)
    # Add other fields as necessary

# Setup the database connection
engine = create_engine('sqlite:///options.db')
Base.metadata.create_all(engine)
