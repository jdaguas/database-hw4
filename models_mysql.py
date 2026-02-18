from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import (Column, Integer, String, DateTime, SmallInteger, 
                        ForeignKey, Numeric)

class Base(DeclarativeBase):
    pass

class Language(Base):
    __tablename__ = "language"

    language_id = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)
    last_update = Column(DateTime, nullable=False)

class Film(Base):
    __tablename__ = "film"

    film_id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    release_year = Column(Integer, nullable=True)
    language_id = Column(Integer, ForeignKey("language.language_id"), nullable=False)
    rating = Column(String(10), nullable=True)
    length = Column(Integer, nullable=True)
    last_update = Column(DateTime, nullable=False)

class Actor(Base):
    __tablename__ = "actor"
    actor_id = Column(Integer, primary_key=True)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    last_update = Column(DateTime, nullable=False)

class Category(Base):
    __tablename__ = "category"
    category_id = Column(SmallInteger, primary_key=True)
    name = Column(String(25), nullable=False)
    last_update = Column(DateTime, nullable=False)

class FilmActor(Base):
    __tablename__ = "film_actor"
    actor_id = Column(Integer, primary_key=True)
    film_id = Column(Integer, primary_key=True)
    last_update = Column(DateTime, nullable=False)

class FilmCategory(Base):
    __tablename__ = "film_category"
    film_id = Column(Integer, primary_key=True)
    category_id = Column(SmallInteger, primary_key=True)
    last_update = Column(DateTime, nullable=False)

class Country(Base):
    __tablename__ = "country"
    country_id = Column(SmallInteger, primary_key=True)
    country = Column(String(50), nullable=False)
    last_update = Column(DateTime, nullable=False)

class City(Base):
    __tablename__ = "city"
    city_id = Column(SmallInteger, primary_key=True)
    city = Column(String(50), nullable=False)
    country_id = Column(SmallInteger, nullable=False)
    last_update = Column(DateTime, nullable=False)

class Address(Base):
    __tablename__ = "address"
    address_id = Column(SmallInteger, primary_key=True)
    city_id = Column(SmallInteger, nullable=False)
    last_update = Column(DateTime, nullable=False)

class Store(Base):
    __tablename__ = "store"
    store_id = Column(SmallInteger, primary_key=True)
    address_id = Column(SmallInteger, nullable=False)
    last_update = Column(DateTime, nullable=False)

class Customer(Base):
    __tablename__ = "customer"
    customer_id = Column(SmallInteger, primary_key=True)
    store_id = Column(SmallInteger, nullable=False)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    active = Column(Integer, nullable=False)
    address_id = Column(SmallInteger, nullable=False)
    last_update = Column(DateTime, nullable=False)

class Inventory(Base):
    __tablename__ = "inventory"

    inventory_id = Column(Integer, primary_key=True)
    film_id = Column(Integer, nullable=False)
    store_id = Column(SmallInteger, nullable=False)
    last_update = Column(DateTime, nullable=False)


class Rental(Base):
    __tablename__ = "rental"

    rental_id = Column(Integer, primary_key=True)
    rental_date = Column(DateTime, nullable=False)
    inventory_id = Column(Integer, nullable=False)
    customer_id = Column(SmallInteger, nullable=False)
    return_date = Column(DateTime, nullable=True)
    staff_id = Column(SmallInteger, nullable=False)
    last_update = Column(DateTime, nullable=False)


class Payment(Base):
    __tablename__ = "payment"

    payment_id = Column(Integer, primary_key=True)
    customer_id = Column(SmallInteger, nullable=False)
    staff_id = Column(SmallInteger, nullable=False)
    rental_id = Column(Integer, nullable=False)
    amount = Column(Numeric(5, 2), nullable=False) 
    payment_date = Column(DateTime, nullable=False)
    last_update = Column(DateTime, nullable=False)


class Staff(Base):
    __tablename__ = "staff"

    staff_id = Column(SmallInteger, primary_key=True)
    store_id = Column(SmallInteger, nullable=False)
    last_update = Column(DateTime, nullable=False)
