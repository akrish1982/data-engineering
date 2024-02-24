from sqlalchemy.orm import sessionmaker
from models import engine, OptionData

Session = sessionmaker(bind=engine)

def save_option_data(option_data):
    session = Session()
    try:
        session.add(option_data)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
