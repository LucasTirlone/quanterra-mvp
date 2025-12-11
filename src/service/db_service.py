import os
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from models import Base, Location, LocationEvent, ChainScrape

logger = logging.getLogger(__name__)

session = None

def get_db_session():
    global session
    if session is None:
        engine = get_engine()
        create_database_schema_if_not_exists(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
    return session


def get_engine():
    host: str = os.getenv("DB_HOST")
    port: int = int(os.getenv("DB_PORT"))
    name: str = os.getenv("DB_NAME")
    user: str = os.getenv("DB_USER")
    password: str = os.getenv("DB_PASS")
    
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url)


def create_database_schema_if_not_exists(engine):
    try:
        Base.metadata.create_all(engine)
        logger.info("Schema do banco de dados criado com sucesso.")
    except Exception as e:
        logger.error(f"Erro fatal ao criar o schema do banco de dados: {e}")
        raise 