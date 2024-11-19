from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from datetime import datetime, timedelta

Base = declarative_base()

def preapation_DB(name_table):
    # Динамічно створюємо клас з потрібною назвою таблиці
    class DynamicTable(Base):
        __tablename__ = name_table
        __table_args__ = {'extend_existing': True}
        id = Column(Integer, primary_key=True, autoincrement=True)
        R = Column(Float)
        timestamp = Column(DateTime(timezone=True))

    return DynamicTable

def connect_DB(host: str = None, user: str = None, password: str = None, database: str = None):
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
    Session = sessionmaker(bind=engine)
    session = Session()
    return session, engine

def add_register_record(
        city: str, 
        time: datetime, 
        R: float = None,
        session=None, engine=None
        ):
    
    inspector = inspect(engine)

    if session is None or engine is None:
        return
    
    if city not in inspector.get_table_names():
        print(f"Table '{city}' does not exist in the database.")
        return False  # Завершуємо функцію, якщо таблиця не існує

    # Створюємо динамічну модель для потрібного міста
    DataBase = preapation_DB(city)

    # Створюємо таблицю, якщо вона ще не існує
    new_record = DataBase(
        R=R, 
        timestamp=time
        )
        
    session.add(new_record)
    session.commit()

def disconnect(session):
    session.close()

# def get_daily_averages(town, session, engine):
#     # Визначаємо час доби тому
#     one_day_ago = datetime.utcnow() - timedelta(days=1)

#     # Створюємо динамічну модель для конкретного міста
#     DataBase = create_dynamic_model(town)

#     # Виконуємо запит до бази для обчислення середніх значень
#     averages = session.query(
#         func.avg(DataBase.CO).label('CO'),
#         func.avg(DataBase.SO2).label('SO2'),
#         func.avg(DataBase.NO2).label('NO2'),
#         func.avg(DataBase.NO).label('NO'),
#         func.avg(DataBase.H2S).label('H2S'),
#         func.avg(DataBase.O3).label('O3'),
#         func.avg(DataBase.NH3).label('NH3'),
#         func.avg(DataBase.PM2_5).label('PM2_5'),
#         func.avg(DataBase.PM10).label('PM10')
#     ).filter(DataBase.timestamp >= one_day_ago).first()

#     # Повертаємо середні значення
#     return {
#     'CO': round(averages.CO, 3) if averages.CO is not None else None,
#     'SO2': round(averages.SO2, 3) if averages.SO2 is not None else None,
#     'NO2': round(averages.NO2, 3) if averages.NO2 is not None else None,
#     'NO': round(averages.NO, 3) if averages.NO is not None else None,
#     'H2S': round(averages.H2S, 3) if averages.H2S is not None else None,
#     'O3': round(averages.O3, 3) if averages.O3 is not None else None,
#     'NH3': round(averages.NH3, 3) if averages.NH3 is not None else None,
#     'PM2_5': round(averages.PM2_5, 3) if averages.PM2_5 is not None else None,
#     'PM10': round(averages.PM10, 3) if averages.PM10 is not None else None
# }