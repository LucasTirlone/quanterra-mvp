import os
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from models import Base, Location, LocationEvent, ChainScrape

logger = logging.getLogger(__name__)

def get_db_session():
    engine = get_engine()
    create_database_schema_if_not_exists(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def get_engine():
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "quanterra-mvp")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    sslmode = os.getenv("DB_SSLMODE", "require")  # RDS exige SSL

    if not host or not user or not password:
        raise RuntimeError("Missing DB env vars: DB_HOST/DB_USER/DB_PASS must be set (or loaded via update_secrets).")
    
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}?sslmode={sslmode}"
    return create_engine(url, pool_pre_ping=True)


def create_database_schema_if_not_exists(engine):
    try:
        Base.metadata.create_all(engine)
        logger.info("Schema do banco de dados criado com sucesso.")
    except Exception as e:
        logger.error(f"Erro fatal ao criar o schema do banco de dados: {e}")
        raise 
