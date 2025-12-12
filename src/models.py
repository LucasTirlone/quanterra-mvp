from sqlalchemy import Column, Text, BigInteger, Integer, Date, Boolean, Float, CHAR, Time, TIMESTAMP, func, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, relationship
from datetime import date
from typing import Any, Dict, List

Base = declarative_base()

class ChainScrape(Base):
    __tablename__ = 'chain_scrapes'
    id = Column(BigInteger, primary_key=True)
    chain_id = Column(Integer)
    chain_name = Column(Text, nullable=False)
    collection_id = Column(Integer, nullable=True)
    scrape_date = Column(Date, nullable=False)
    scrape_time = Column(Time, nullable=False)
    us_location_count = Column(Integer)
    location_count = Column(Integer)
    run_check_count = Column(Integer)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint('chain_id', 'scrape_date', name='chain_scrapes_chain_id_scrape_date_key'),
    )

    @classmethod
    def upsert(cls, session: Session, data: dict):
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            constraint='chain_scrapes_chain_id_scrape_date_key',
            set_=data
        )
        session.execute(stmt)
        session.commit()


    @classmethod
    def get_all_by_collection_id(cls, session, collection_id: int, start_date: date, end_date: date) -> List['ChainScrape']:
        query = session.query(cls).filter(
            cls.collection_id == collection_id
        )
        
        query = query.filter(cls.scrape_date >= start_date)
        query = query.filter(cls.scrape_date <= end_date)
    
        return query.all()


class Location(Base):
    __tablename__ = 'locations'
    synthetic_location_id = Column(Text, primary_key=True)
    chain_id = Column(Integer, nullable=False)
    chain_name = Column(Text, nullable=False)
    chain_slug = Column(Text, nullable=False)
    store_name = Column(Text)
    partner_hash_id = Column(Text)
    address_normalized = Column(Text, nullable=False)
    address_complement = Column(Text)
    store_number = Column(Text)
    phone_number = Column(Text)
    parent_chain_id = Column(Integer)
    parent_chain_name = Column(Text)
    coming_soon = Column(Boolean, default=False)
    store_hours = Column(Text)
    status = Column(Text)
    opened_at_estimated = Column(Date)
    closed_at_estimated = Column(Date)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    site_id = Column(Text)
    city = Column(Text, nullable=False)
    state = Column(Text, nullable=False)
    zip = Column(CHAR(5))
    last_event_date = Column(Date)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    @classmethod
    def upsert(cls, session: Session, data: dict):
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['synthetic_location_id'],
            set_=data
        )
        session.execute(stmt)
        session.commit()
        
        
    @classmethod
    def update_status_by_chain_id(cls, session, chain_id: int, new_status: str):
        update_fields = {
            cls.status: new_status,
            cls.updated_at: func.now()
        }
        
        current_date = date.today()

        if new_status == "OPEN":
            update_fields[cls.opened_at_estimated] = current_date
            update_fields[cls.closed_at_estimated] = None
        elif new_status == "CLOSE":
            update_fields[cls.closed_at_estimated] = current_date
            update_fields[cls.opened_at_estimated] = None
        else:
            return
        
        return session.query(cls).filter(
            cls.chain_id == chain_id
        ).update(
            update_fields,
            synchronize_session='fetch'
        )
    
    @classmethod
    def close_when_limit_expires(cls, session, limit_date : date):
        update_fields = {
            cls.status: "CLOSE",
            cls.closed_at_estimated: date.today()
        }
        
        return session.query(cls).filter(
            cls.last_event_date < limit_date
        ).update(
            update_fields,
            synchronize_session='fetch'
        )
        
        
    @classmethod
    def get_all_by_chain_id(cls, session, chain_id: int) -> List['Location']:
        query = session.query(cls).filter(
            cls.chain_id == chain_id
        )
        
        return query.all()
     
        
    def update_status(self, session: Session, status: str, date: date):
        self.status = status
        self.updated_at = func.now()
        
        if "OPEN" == status:
            self.opened_at_estimated = date
            self.closed_at_estimated = None
        else:
            self.closed_at_estimated = date
        
        session.commit()
        
    
    def update_last_event_date(self, session: Session, new_last_event_date):
        self.updated_at = func.now()
        
        if not self.last_event_date or self.last_event_date < new_last_event_date:
            self.last_event_date = new_last_event_date
        
        session.commit()


class LocationEvent(Base):
    __tablename__ = 'location_events'
    id = Column(BigInteger, primary_key=True)
    synthetic_location_id = Column(Text, nullable=False)
    chain_id = Column(Integer, nullable=False)
    event_type = Column(Text, nullable=False)
    event_date_estimated = Column(Date)
    suspected_hash_change = Column(Boolean, default=False)
    remodel_type = Column(Text)
    scrape_date = Column(Date)
    current_opened_at_estimated = Column(Date)
    current_closed_at_estimated = Column(Date)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    
    last_location_event_id = Column(
        BigInteger, 
        ForeignKey('location_events.id'),  
        index=True
    )
    last_location_event = relationship("LocationEvent", remote_side=[id])

    __table_args__ = (
        UniqueConstraint('synthetic_location_id', 'scrape_date', 'event_type', name='ux_events_sid_endtype'),
    )

    @classmethod
    def upsert(cls, session: Session, data: dict):
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            constraint='ux_events_sid_endtype',
            set_=data
        )
        session.execute(stmt)
        session.commit()
    
    
    @classmethod
    def get_all_by_date_range(cls, session, start_date: date, end_date: date) -> List['LocationEvent']:
        query = session.query(cls).filter(cls.scrape_date >= start_date).filter(cls.scrape_date <= end_date)
        return query.all()
        
        
    def update_remodel(self, session: Session, remodel_type: str):
        self.last_update = func.now()
        self.remodel_type = remodel_type
        session.commit()
        

class UsRegion(Base):
    __tablename__ = 'us_region'
    zip = Column(Integer, primary_key=True)
    division = Column(Text)
    region = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint('zip', name='zip_key'),
    )

    @classmethod
    def upsert(cls, session: Session, data: dict):
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            constraint='zip_key',
            set_=data
        )
        session.execute(stmt)
        session.commit()
        
    @classmethod
    def get_by_zip(cls, session, zip: int) -> 'UsRegion':
        return session.query(cls).filter(
            cls.zip == zip
        ).one_or_none()
        

class QualityReport(Base):
    __tablename__ = 'quality_report'
    id = Column(BigInteger, primary_key=True)
    file_name = Column(Text, nullable=False)
    collection_id = Column(Integer, nullable=False)
    row_number = Column(Integer, nullable=False)
    chain_id = Column(Text, nullable=False)
    scrape_date = Column(Date, nullable=False)
    valitation_result = Column(Text, nullable=False)
    invalid_columns = Column(Text)
    blank_columns = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint('file_name', 'scrape_date', name='quality_report_file_name_scrape_date_key'),
    )

    @classmethod
    def upsert(cls, session: Session, data: dict):
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            constraint='quality_report_file_name_scrape_date_key',
            set_=data
        )
        session.execute(stmt)
        session.commit()


class FileEventLog(Base):
    __tablename__ = 'file_event_log'
    id = Column(BigInteger, primary_key=True)
    file_name = Column(Text, nullable=False)
    collection_id = Column(Integer, nullable=False)
    status = Column(Text, nullable=False)
    run_date = Column(Date, nullable=False)
    error_message = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint('file_name', 'run_date', name='file_event_log_file_name_run_date_key'),
    )

    @classmethod
    def upsert(cls, session: Session, data: dict):
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            constraint='file_event_log_file_name_run_date_key',
            set_=data
        )
        session.execute(stmt)
        session.commit()


class ParentChain(Base):
    __tablename__ = "parent_chains"

    chain_id = Column(Integer, primary_key=True)
    chain_name = Column(Text, nullable=False)
    chain_status = Column(Text)
    url = Column(Text)

    parent_chain_id = Column(Text)
    parent_chain_name = Column(Text)
    stock_ticker = Column(Text)

    manual_change = Column(Boolean, default=False)
    change_fields = Column(Text)
    original_values = Column(Text)
    change_reason = Column(Text)
    modified_by = Column(Text)
    modified_date = Column(Date)
    archive_record = Column(Boolean, default=False)
    upload_timestamp = Column(TIMESTAMP(timezone=True))
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    @classmethod
    def upsert(cls, session: Session, data: Dict[str, Any]) -> None:
        """
        Upsert of chain_id (natural key).
        """
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["chain_id"],
            set_=data,
        )
        session.execute(stmt)
        session.commit()


class Landlord(Base):
    __tablename__ = "landlords"

    landlord_id = Column(Text, primary_key=True)
    landlord_name = Column(Text, nullable=False)

    landlord_status = Column(Text)
    url = Column(Text)
    sic_code = Column(Text)
    naics_code = Column(Text)
    primary_category = Column(Text)
    categories = Column(Text)
    countries = Column(Text)
    property_count = Column(Integer)

    is_public = Column(Boolean, default=False)
    stock_ticker = Column(Text)
    property_sector = Column(Text)
    property_subsector = Column(Text)
    index_name = Column(Text)
    region_coverage = Column(Text)
    property_url = Column(Text)

    archive_record = Column(Boolean, default=False)
    manual_change = Column(Boolean, default=False)
    change_fields = Column(Text)
    original_values = Column(Text)
    change_reason = Column(Text)
    modified_by = Column(Text)
    modified_date = Column(Date)
    upload_timestamp = Column(TIMESTAMP(timezone=True))
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    @classmethod
    def upsert(cls, session: Session, data: Dict[str, Any]) -> None:
        """
        Upsert by landlord_id.
        """
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["landlord_id"],
            set_=data,
        )
        session.execute(stmt)
        session.commit()


class Center(Base):
    __tablename__ = "centers"

    site_id = Column(Text, primary_key=True)
    title = Column(Text, nullable=False)

    center_type = Column(Text)
    format = Column(Text)

    address = Column(Text)
    address2 = Column(Text)
    city = Column(Text)
    region = Column(Text)
    postal_code = Column(Text)
    country = Column(Text)

    latitude = Column(Float)
    longitude = Column(Float)
    gla = Column(Float)
    units = Column(Integer)
    year_opened = Column(Integer)
    location_count = Column(Integer)
    anchor_count = Column(Integer)
    anchor_chains = Column(Text)

    country_std = Column(Text)
    state_std = Column(Text)
    postal_code_std = Column(Text)

    archive_record = Column(Boolean, default=False)
    manual_change = Column(Boolean, default=False)
    change_field = Column(Text)
    original = Column(Text)
    change_reason = Column(Text)
    modified_by = Column(Text)
    modified_date = Column(Date)
    upload_timestamp = Column(TIMESTAMP(timezone=True))
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    @classmethod
    def upsert(cls, session: Session, data: Dict[str, Any]) -> None:
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["site_id"],
            set_=data,
        )
        session.execute(stmt)
        session.commit()
        

class CenterLandlord(Base):
    __tablename__ = "center_landlords"

    id = Column(BigInteger, primary_key=True)
    site_id = Column(Text, nullable=False)
    landlord_id = Column(Text, nullable=False)
    ownership_pct = Column(Float)  # ex.: 100.0, 50.0, etc.

    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "site_id", "landlord_id", name="center_landlords_site_landlord_key"
        ),
    )

    @classmethod
    def upsert(cls, session: Session, data: Dict[str, Any]) -> None:
        """
        Upsert by (site_id, landlord_id).
        """
        stmt = insert(cls).values(**data)
        stmt = stmt.on_conflict_do_update(
            constraint="center_landlords_site_landlord_key",
            set_=data,
        )
        session.execute(stmt)
        session.commit()
