from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import (Column, Integer, Date, Boolean, String, DateTime, 
                        UniqueConstraint, Index, Float)
from datetime import datetime



class BaseSQLite(DeclarativeBase):
    pass

class SyncState(BaseSQLite):
    __tablename__ = "sync_state"
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(64), nullable=False, unique=True)
    last_synced_at = Column(DateTime, nullable=True) 

class DimDate(BaseSQLite):
    __tablename__ = "dim_date"

    date_key = Column(Integer, primary_key=True) 
    date = Column(Date, nullable=False, unique=True)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False) 
    is_weekend = Column(Boolean, nullable=False)

class DimFilm(BaseSQLite):
    __tablename__ = "dim_film"

    film_key = Column(Integer, primary_key=True, autoincrement=True)  
    film_id = Column(Integer, nullable=False, unique=True)            
    title = Column(String(255), nullable=False)
    rating = Column(String(10), nullable=True)
    length = Column(Integer, nullable=True)
    language = Column(String(20), nullable=False)
    release_year = Column(Integer, nullable=True)
    last_update = Column(DateTime, nullable=False)

Index("ix_dim_film_film_id", DimFilm.film_id)
Index("ix_dim_film_last_update", DimFilm.last_update)

class DimActor(BaseSQLite):
    __tablename__ = "dim_actor"
    actor_key = Column(Integer, primary_key=True, autoincrement=True)
    actor_id = Column(Integer, nullable=False, unique=True)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    last_update = Column(DateTime, nullable=False)

Index("ix_dim_actor_actor_id", DimActor.actor_id)
Index("ix_dim_actor_last_update", DimActor.last_update)


class DimCategory(BaseSQLite):
    __tablename__ = "dim_category"
    category_key = Column(Integer, primary_key=True, autoincrement=True)
    category_id = Column(Integer, nullable=False, unique=True)
    name = Column(String(25), nullable=False)
    last_update = Column(DateTime, nullable=False)

Index("ix_dim_category_category_id", DimCategory.category_id)
Index("ix_dim_category_last_update", DimCategory.last_update)


class DimStore(BaseSQLite):
    __tablename__ = "dim_store"
    store_key = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, nullable=False, unique=True)
    city = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)
    last_update = Column(DateTime, nullable=False)

Index("ix_dim_store_store_id", DimStore.store_id)
Index("ix_dim_store_last_update", DimStore.last_update)


class DimCustomer(BaseSQLite):
    __tablename__ = "dim_customer"
    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, nullable=False, unique=True)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    active = Column(Boolean, nullable=False)
    city = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)
    last_update = Column(DateTime, nullable=False)

Index("ix_dim_customer_customer_id", DimCustomer.customer_id)
Index("ix_dim_customer_last_update", DimCustomer.last_update)


class BridgeFilmActor(BaseSQLite):
    __tablename__ = "bridge_film_actor"
    film_key = Column(Integer, primary_key=True)
    actor_key = Column(Integer, primary_key=True)

class BridgeFilmCategory(BaseSQLite):
    __tablename__ = "bridge_film_category"
    film_key = Column(Integer, primary_key=True)
    category_key = Column(Integer, primary_key=True)


class FactRental(BaseSQLite):
    __tablename__ = "fact_rental"

    fact_rental_key = Column(Integer, primary_key=True, autoincrement=True) 
    rental_id = Column(Integer, nullable=False, unique=True)

    date_key_rented = Column(Integer, nullable=False)
    date_key_returned = Column(Integer, nullable=True)

    film_key = Column(Integer, nullable=False)
    store_key = Column(Integer, nullable=False)
    customer_key = Column(Integer, nullable=False)

    staff_id = Column(Integer, nullable=False)
    rental_duration_days = Column(Integer, nullable=True)

Index("ix_fact_rental_rental_id", FactRental.rental_id)
Index("ix_fact_rental_date_key_rented", FactRental.date_key_rented)
Index("ix_fact_rental_store_key", FactRental.store_key)
Index("ix_fact_rental_customer_key", FactRental.customer_key)
Index("ix_fact_rental_film_key", FactRental.film_key)


class FactPayment(BaseSQLite):
    __tablename__ = "fact_payment"

    fact_payment_key = Column(Integer, primary_key=True, autoincrement=True)

    payment_id = Column(Integer, nullable=False, unique=True)

    date_key_paid = Column(Integer, nullable=False)

    customer_key = Column(Integer, nullable=False)
    store_key = Column(Integer, nullable=False)

    staff_id = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)

Index("ix_fact_payment_payment_id", FactPayment.payment_id)
Index("ix_fact_payment_date_key_paid", FactPayment.date_key_paid)
Index("ix_fact_payment_store_key", FactPayment.store_key)
Index("ix_fact_payment_customer_key", FactPayment.customer_key)
Index("ix_fact_payment_staff_id", FactPayment.staff_id)


