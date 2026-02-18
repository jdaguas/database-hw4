from datetime import date, timedelta, datetime
from sqlalchemy import text, func
from sqlalchemy.orm import Session

from database import (
    get_mysql_engine,
    get_sqlite_engine,
    get_sqlite_session,
    get_mysql_session
)

from models_sqlite import (
  BaseSQLite, DimDate, DimFilm, DimActor, DimCategory, DimStore, DimCustomer,
    BridgeFilmActor, BridgeFilmCategory, SyncState, FactRental, FactPayment
)

from models_mysql import (
   Actor, Category, Store, Customer,
    Address, City, Country,
    FilmActor, FilmCategory,
    Film, Language, Rental, Payment, Staff, Inventory
)


def get_last_sync(sqlite_session, table_name: str):
    row = sqlite_session.query(SyncState).filter_by(table_name=table_name).first()
    return row.last_synced_at if row else None

def set_last_sync(sqlite_session, table_name: str, ts: datetime):
    row = sqlite_session.query(SyncState).filter_by(table_name=table_name).first()
    if row:
        row.last_synced_at = ts
    else:
        sqlite_session.add(SyncState(table_name=table_name, last_synced_at=ts))

def make_date_key(dt) -> int | None:
    if dt is None:
        return None
    return int(dt.strftime("%Y%m%d"))


def populate_dim_date(session, start: date, end: date) -> None:
    cur = start
    while cur <= end:
        date_key = int(cur.strftime("%Y%m%d"))
        session.merge(DimDate(
            date_key=date_key,
            date=cur,
            year=cur.year,
            quarter=(cur.month - 1) // 3 + 1,
            month=cur.month,
            day_of_month=cur.day,
            day_of_week=cur.isoweekday(),
            is_weekend=(cur.isoweekday() >= 6),
        ))
        cur += timedelta(days=1)


def full_load() -> None:
    sqlite_engine = get_sqlite_engine()
    BaseSQLite.metadata.create_all(sqlite_engine)

    mysql_session = get_mysql_session()
    sqlite_session = get_sqlite_session()

    try:
        full_load_dim_film(mysql_session, sqlite_session)
        full_load_dim_actor(mysql_session, sqlite_session)
        full_load_dim_category(mysql_session, sqlite_session)
        full_load_dim_store(mysql_session, sqlite_session)
        full_load_dim_customer(mysql_session, sqlite_session)
        full_load_bridge_film_actor(mysql_session, sqlite_session)
        full_load_bridge_film_category(mysql_session, sqlite_session)
        full_load_fact_rental(mysql_session, sqlite_session)
        full_load_fact_payment(mysql_session, sqlite_session)
    finally:
        mysql_session.close()
        sqlite_session.close()


def full_load_dim_film(mysql_session, sqlite_session):
    sqlite_session.query(DimFilm).delete()

    rows = (
        mysql_session.query(Film, Language)
        .join(Language, Film.language_id == Language.language_id)
        .all()
    )

    for film, lang in rows:
        sqlite_session.add(DimFilm(
            film_id=film.film_id,
            title=film.title,
            rating=film.rating,
            length=film.length,
            language=lang.name,
            release_year=film.release_year,
            last_update=film.last_update,
        ))

    sqlite_session.commit()
    print(f"Loaded dim_film: {len(rows)} rows")

def full_load_dim_actor(mysql_session, sqlite_session):
    sqlite_session.query(DimActor).delete()
    rows = mysql_session.query(Actor).all()
    for a in rows:
        sqlite_session.add(DimActor(
            actor_id=a.actor_id,
            first_name=a.first_name,
            last_name=a.last_name,
            last_update=a.last_update
        ))
    sqlite_session.commit()
    print(f"Loaded dim_actor: {len(rows)} rows")


def full_load_dim_category(mysql_session, sqlite_session):
    sqlite_session.query(DimCategory).delete()
    rows = mysql_session.query(Category).all()
    for c in rows:
        sqlite_session.add(DimCategory(
            category_id=int(c.category_id),
            name=c.name,
            last_update=c.last_update
        ))
    sqlite_session.commit()
    print(f"Loaded dim_category: {len(rows)} rows")


def full_load_dim_store(mysql_session, sqlite_session):
    sqlite_session.query(DimStore).delete()

    rows = (
        mysql_session.query(Store, Address, City, Country)
        .join(Address, Store.address_id == Address.address_id)
        .join(City, Address.city_id == City.city_id)
        .join(Country, City.country_id == Country.country_id)
        .all()
    )

    for s, a, ci, co in rows:
        sqlite_session.add(DimStore(
            store_id=int(s.store_id),
            city=ci.city,
            country=co.country,
            last_update=s.last_update
        ))

    sqlite_session.commit()
    print(f"Loaded dim_store: {len(rows)} rows")


def full_load_dim_customer(mysql_session, sqlite_session):
    sqlite_session.query(DimCustomer).delete()

    rows = (
        mysql_session.query(Customer, Address, City, Country)
        .join(Address, Customer.address_id == Address.address_id)
        .join(City, Address.city_id == City.city_id)
        .join(Country, City.country_id == Country.country_id)
        .all()
    )

    for cust, addr, ci, co in rows:
        sqlite_session.add(DimCustomer(
            customer_id=int(cust.customer_id),
            first_name=cust.first_name,
            last_name=cust.last_name,
            active=bool(cust.active),
            city=ci.city,
            country=co.country,
            last_update=cust.last_update
        ))

    sqlite_session.commit()
    print(f"Loaded dim_customer: {len(rows)} rows")


def full_load_bridge_film_actor(mysql_session, sqlite_session):
    sqlite_session.query(BridgeFilmActor).delete()
    film_map, actor_map, _, _, _ = build_key_maps(sqlite_session)

    rows = mysql_session.query(FilmActor).all()
    missing = 0

    for fa in rows:
        fk = film_map.get(fa.film_id)
        ak = actor_map.get(fa.actor_id)
        if fk is None or ak is None:
            missing += 1
            continue
        sqlite_session.add(BridgeFilmActor(film_key=fk, actor_key=ak))

    sqlite_session.commit()
    print(f"Loaded bridge_film_actor: {len(rows) - missing} rows (skipped {missing})")


def full_load_bridge_film_category(mysql_session, sqlite_session):
    sqlite_session.query(BridgeFilmCategory).delete()
    film_map, _, cat_map, _, _ = build_key_maps(sqlite_session)


    rows = mysql_session.query(FilmCategory).all()
    missing = 0

    for fc in rows:
        fk = film_map.get(fc.film_id)
        ck = cat_map.get(int(fc.category_id))
        if fk is None or ck is None:
            missing += 1
            continue
        sqlite_session.add(BridgeFilmCategory(film_key=fk, category_key=ck))

    sqlite_session.commit()
    print(f"Loaded bridge_film_category: {len(rows) - missing} rows (skipped {missing})")


def full_load_fact_rental(mysql_session, sqlite_session):
    sqlite_session.query(FactRental).delete()

    film_map, _, _, store_map, customer_map = build_key_maps(sqlite_session)

    rows = (
        mysql_session.query(Rental, Inventory)
        .join(Inventory, Rental.inventory_id == Inventory.inventory_id)
        .all()
    )

    missing = 0
    for r, inv in rows:
        film_key = film_map.get(inv.film_id)
        store_key = store_map.get(inv.store_id)
        customer_key = customer_map.get(r.customer_id)

        if film_key is None or store_key is None or customer_key is None:
            missing += 1
            continue

        rented_key = make_date_key(r.rental_date)
        returned_key = make_date_key(r.return_date)

        rental_duration_days = None
        if r.return_date is not None and r.rental_date is not None:
            rental_duration_days = (r.return_date.date() - r.rental_date.date()).days

        sqlite_session.add(FactRental(
            rental_id=r.rental_id,
            date_key_rented=rented_key,
            date_key_returned=returned_key,
            film_key=film_key,
            store_key=store_key,
            customer_key=customer_key,
            staff_id=r.staff_id,
            rental_duration_days=rental_duration_days,
        ))

    sqlite_session.commit()
    print(f"Loaded fact_rental: {len(rows) - missing} rows (skipped {missing})")

def full_load_fact_payment(mysql_session, sqlite_session):
    sqlite_session.query(FactPayment).delete()

    _, _, _, store_map, customer_map = build_key_maps(sqlite_session)

    rows = (
        mysql_session.query(Payment, Staff)
        .join(Staff, Payment.staff_id == Staff.staff_id)
        .all()
    )

    missing = 0
    for p, st in rows:
        store_key = store_map.get(st.store_id)
        customer_key = customer_map.get(p.customer_id)

        if store_key is None or customer_key is None:
            missing += 1
            continue

        paid_key = make_date_key(p.payment_date)

        sqlite_session.add(FactPayment(
            payment_id=p.payment_id,
            date_key_paid=paid_key,
            customer_key=customer_key,
            store_key=store_key,
            staff_id=p.staff_id,
            amount=float(p.amount),
        ))

    sqlite_session.commit()
    print(f"Loaded fact_payment: {len(rows) - missing} rows (skipped {missing})")



def build_key_maps(sqlite_session):
    film_map = {r.film_id: r.film_key
                for r in sqlite_session.query(DimFilm.film_id, DimFilm.film_key).all()}
    actor_map = {r.actor_id: r.actor_key
                 for r in sqlite_session.query(DimActor.actor_id, DimActor.actor_key).all()}
    cat_map  = {r.category_id: r.category_key
                for r in sqlite_session.query(DimCategory.category_id, DimCategory.category_key).all()}

    store_map = {r.store_id: r.store_key
                 for r in sqlite_session.query(DimStore.store_id, DimStore.store_key).all()}
    customer_map = {r.customer_id: r.customer_key
                    for r in sqlite_session.query(DimCustomer.customer_id, DimCustomer.customer_key).all()}

    return film_map, actor_map, cat_map, store_map, customer_map


def incremental() -> None:
    mysql_session = get_mysql_session()
    sqlite_session = get_sqlite_session()

    try:
        changed_films = incremental_dim_film(mysql_session, sqlite_session)
        changed_actors = incremental_dim_actor(mysql_session, sqlite_session)
        changed_cats = incremental_dim_category(mysql_session, sqlite_session)
        changed_stores = incremental_dim_store(mysql_session, sqlite_session)
        changed_customers = incremental_dim_customer(mysql_session, sqlite_session)

        incremental_bridge_film_actor(mysql_session, sqlite_session, changed_films, changed_actors)
        incremental_bridge_film_category(mysql_session, sqlite_session, changed_films, changed_cats)

        incremental_fact_rental(mysql_session, sqlite_session)
        incremental_fact_payment(mysql_session, sqlite_session)

    finally:
        mysql_session.close()
        sqlite_session.close()


def incremental_dim_film(mysql_session, sqlite_session) -> set[int]:
    last_sync = get_last_sync(sqlite_session, "film")

    q = (
        mysql_session.query(Film, Language)
        .join(Language, Film.language_id == Language.language_id)
    )
    if last_sync:
        q = q.filter(Film.last_update > last_sync)

    rows = q.all()
    changed_film_ids: set[int] = set()
    max_ts = last_sync

    for film, lang in rows:
        changed_film_ids.add(int(film.film_id))

        existing = sqlite_session.query(DimFilm).filter_by(film_id=film.film_id).first()
        if existing:
            existing.title = film.title
            existing.rating = film.rating
            existing.length = film.length
            existing.language = lang.name
            existing.release_year = film.release_year
            existing.last_update = film.last_update
        else:
            sqlite_session.add(DimFilm(
                film_id=film.film_id,
                title=film.title,
                rating=film.rating,
                length=film.length,
                language=lang.name,
                release_year=film.release_year,
                last_update=film.last_update,
            ))

        if max_ts is None or film.last_update > max_ts:
            max_ts = film.last_update

    if max_ts:
        set_last_sync(sqlite_session, "film", max_ts)

    sqlite_session.commit()
    print(f"Incremental dim_film: {len(rows)} rows")
    return changed_film_ids

def incremental_dim_actor(mysql_session, sqlite_session) -> set[int]:
    last_sync = get_last_sync(sqlite_session, "actor")

    q = mysql_session.query(Actor)
    if last_sync:
        q = q.filter(Actor.last_update > last_sync)

    rows = q.all()
    changed_actor_ids: set[int] = set()
    max_ts = last_sync

    for a in rows:
        changed_actor_ids.add(int(a.actor_id))

        existing = sqlite_session.query(DimActor).filter_by(actor_id=a.actor_id).first()
        if existing:
            existing.first_name = a.first_name
            existing.last_name = a.last_name
            existing.last_update = a.last_update
        else:
            sqlite_session.add(DimActor(
                actor_id=a.actor_id,
                first_name=a.first_name,
                last_name=a.last_name,
                last_update=a.last_update,
            ))

        if max_ts is None or a.last_update > max_ts:
            max_ts = a.last_update

    if max_ts:
        set_last_sync(sqlite_session, "actor", max_ts)

    sqlite_session.commit()
    print(f"Incremental dim_actor: {len(rows)} rows")
    return changed_actor_ids
def incremental_dim_category(mysql_session, sqlite_session) -> set[int]:
    last_sync = get_last_sync(sqlite_session, "category")

    q = mysql_session.query(Category)
    if last_sync:
        q = q.filter(Category.last_update > last_sync)

    rows = q.all()
    changed_category_ids: set[int] = set()
    max_ts = last_sync

    for c in rows:
        cid = int(c.category_id)
        changed_category_ids.add(cid)

        existing = sqlite_session.query(DimCategory).filter_by(category_id=cid).first()
        if existing:
            existing.name = c.name
            existing.last_update = c.last_update
        else:
            sqlite_session.add(DimCategory(
                category_id=cid,
                name=c.name,
                last_update=c.last_update,
            ))

        if max_ts is None or c.last_update > max_ts:
            max_ts = c.last_update

    if max_ts:
        set_last_sync(sqlite_session, "category", max_ts)

    sqlite_session.commit()
    print(f"Incremental dim_category: {len(rows)} rows")
    return changed_category_ids

def incremental_dim_store(mysql_session, sqlite_session) -> set[int]:
    last_sync = get_last_sync(sqlite_session, "store")

    q = (
        mysql_session.query(Store, Address, City, Country)
        .join(Address, Store.address_id == Address.address_id)
        .join(City, Address.city_id == City.city_id)
        .join(Country, City.country_id == Country.country_id)
    )
    if last_sync:
        q = q.filter(Store.last_update > last_sync)

    rows = q.all()
    changed_store_ids: set[int] = set()
    max_ts = last_sync

    for s, a, ci, co in rows:
        sid = int(s.store_id)
        changed_store_ids.add(sid)

        existing = sqlite_session.query(DimStore).filter_by(store_id=sid).first()
        if existing:
            existing.city = ci.city
            existing.country = co.country
            existing.last_update = s.last_update
        else:
            sqlite_session.add(DimStore(
                store_id=sid,
                city=ci.city,
                country=co.country,
                last_update=s.last_update,
            ))

        if max_ts is None or s.last_update > max_ts:
            max_ts = s.last_update

    if max_ts:
        set_last_sync(sqlite_session, "store", max_ts)

    sqlite_session.commit()
    print(f"Incremental dim_store: {len(rows)} rows")
    return changed_store_ids

def incremental_dim_customer(mysql_session, sqlite_session) -> set[int]:
    last_sync = get_last_sync(sqlite_session, "customer")

    q = (
        mysql_session.query(Customer, Address, City, Country)
        .join(Address, Customer.address_id == Address.address_id)
        .join(City, Address.city_id == City.city_id)
        .join(Country, City.country_id == Country.country_id)
    )
    if last_sync:
        q = q.filter(Customer.last_update > last_sync)

    rows = q.all()
    changed_customer_ids: set[int] = set()
    max_ts = last_sync

    for cust, addr, ci, co in rows:
        cid = int(cust.customer_id)
        changed_customer_ids.add(cid)

        existing = sqlite_session.query(DimCustomer).filter_by(customer_id=cid).first()
        if existing:
            existing.first_name = cust.first_name
            existing.last_name = cust.last_name
            existing.active = bool(cust.active)
            existing.city = ci.city
            existing.country = co.country
            existing.last_update = cust.last_update
        else:
            sqlite_session.add(DimCustomer(
                customer_id=cid,
                first_name=cust.first_name,
                last_name=cust.last_name,
                active=bool(cust.active),
                city=ci.city,
                country=co.country,
                last_update=cust.last_update,
            ))

        if max_ts is None or cust.last_update > max_ts:
            max_ts = cust.last_update

    if max_ts:
        set_last_sync(sqlite_session, "customer", max_ts)

    sqlite_session.commit()
    print(f"Incremental dim_customer: {len(rows)} rows")
    return changed_customer_ids

def incremental_bridge_film_actor(mysql_session, sqlite_session,
                                 changed_film_ids: set[int],
                                 changed_actor_ids: set[int]) -> None:
    
    if not changed_film_ids and not changed_actor_ids:
        print("Incremental bridge_film_actor: 0 rows (no changes)")
        return


    film_map, actor_map, _, _, _ = build_key_maps(sqlite_session)


    q = mysql_session.query(FilmActor)
    if changed_film_ids and changed_actor_ids:
        q = q.filter((FilmActor.film_id.in_(changed_film_ids)) | (FilmActor.actor_id.in_(changed_actor_ids)))
    elif changed_film_ids:
        q = q.filter(FilmActor.film_id.in_(changed_film_ids))
    else:
        q = q.filter(FilmActor.actor_id.in_(changed_actor_ids))

    rows = q.all()

    if changed_film_ids:
        impacted_film_keys = [film_map.get(fid) for fid in changed_film_ids if film_map.get(fid) is not None]
        if impacted_film_keys:
            sqlite_session.query(BridgeFilmActor).filter(BridgeFilmActor.film_key.in_(impacted_film_keys)).delete(synchronize_session=False)

    if changed_actor_ids:
        impacted_actor_keys = [actor_map.get(aid) for aid in changed_actor_ids if actor_map.get(aid) is not None]
        if impacted_actor_keys:
            sqlite_session.query(BridgeFilmActor).filter(BridgeFilmActor.actor_key.in_(impacted_actor_keys)).delete(synchronize_session=False)

    missing = 0
    for fa in rows:
        fk = film_map.get(int(fa.film_id))
        ak = actor_map.get(int(fa.actor_id))
        if fk is None or ak is None:
            missing += 1
            continue
        sqlite_session.add(BridgeFilmActor(film_key=fk, actor_key=ak))

    sqlite_session.commit()
    print(f"Incremental bridge_film_actor: {len(rows) - missing} rows (skipped {missing})")

def incremental_bridge_film_category(mysql_session, sqlite_session,
                                    changed_film_ids: set[int],
                                    changed_category_ids: set[int]) -> None:
    if not changed_film_ids and not changed_category_ids:
        print("Incremental bridge_film_category: 0 rows (no changes)")
        return

    film_map, _, cat_map, _, _ = build_key_maps(sqlite_session)

    q = mysql_session.query(FilmCategory)
    if changed_film_ids and changed_category_ids:
        q = q.filter((FilmCategory.film_id.in_(changed_film_ids)) | (FilmCategory.category_id.in_(changed_category_ids)))
    elif changed_film_ids:
        q = q.filter(FilmCategory.film_id.in_(changed_film_ids))
    else:
        q = q.filter(FilmCategory.category_id.in_(changed_category_ids))

    rows = q.all()

    if changed_film_ids:
        impacted_film_keys = [film_map.get(fid) for fid in changed_film_ids if film_map.get(fid) is not None]
        if impacted_film_keys:
            sqlite_session.query(BridgeFilmCategory).filter(BridgeFilmCategory.film_key.in_(impacted_film_keys)).delete(synchronize_session=False)

    if changed_category_ids:
        impacted_cat_keys = [cat_map.get(cid) for cid in changed_category_ids if cat_map.get(cid) is not None]
        if impacted_cat_keys:
            sqlite_session.query(BridgeFilmCategory).filter(BridgeFilmCategory.category_key.in_(impacted_cat_keys)).delete(synchronize_session=False)

    missing = 0
    for fc in rows:
        fk = film_map.get(int(fc.film_id))
        ck = cat_map.get(int(fc.category_id))
        if fk is None or ck is None:
            missing += 1
            continue
        sqlite_session.add(BridgeFilmCategory(film_key=fk, category_key=ck))

    sqlite_session.commit()
    print(f"Incremental bridge_film_category: {len(rows) - missing} rows (skipped {missing})")


def incremental_fact_rental(mysql_session, sqlite_session) -> None:
    last_sync = get_last_sync(sqlite_session, "rental")

    film_map, _, _, store_map, customer_map = build_key_maps(sqlite_session)

    q = (
        mysql_session.query(Rental, Inventory)
        .join(Inventory, Rental.inventory_id == Inventory.inventory_id)
    )
    if last_sync:
        q = q.filter(Rental.last_update > last_sync)

    rows = q.all()
    max_ts = last_sync
    processed = 0

    for r, inv in rows:
        film_key = film_map.get(int(inv.film_id))
        store_key = store_map.get(int(inv.store_id))
        customer_key = customer_map.get(int(r.customer_id))

        if film_key is None or store_key is None or customer_key is None:
            continue

        rented_key = make_date_key(r.rental_date)
        returned_key = make_date_key(r.return_date)

        rental_duration_days = None
        if r.return_date and r.rental_date:
            rental_duration_days = (r.return_date.date() - r.rental_date.date()).days

        existing = sqlite_session.query(FactRental).filter_by(rental_id=r.rental_id).first()
        if existing:
            existing.date_key_rented = rented_key
            existing.date_key_returned = returned_key
            existing.film_key = film_key
            existing.store_key = store_key
            existing.customer_key = customer_key
            existing.staff_id = r.staff_id
            existing.rental_duration_days = rental_duration_days
        else:
            sqlite_session.add(FactRental(
                rental_id=r.rental_id,
                date_key_rented=rented_key,
                date_key_returned=returned_key,
                film_key=film_key,
                store_key=store_key,
                customer_key=customer_key,
                staff_id=r.staff_id,
                rental_duration_days=rental_duration_days,
            ))

        processed += 1
        if max_ts is None or r.last_update > max_ts:
            max_ts = r.last_update

    if max_ts:
        set_last_sync(sqlite_session, "rental", max_ts)

    sqlite_session.commit()
    print(f"Incremental fact_rental: {processed} rows processed (source candidates: {len(rows)})")


def incremental_fact_payment(mysql_session, sqlite_session) -> None:
    last_sync = get_last_sync(sqlite_session, "payment")

    _, _, _, store_map, customer_map = build_key_maps(sqlite_session)

    q = (
        mysql_session.query(Payment, Staff)
        .join(Staff, Payment.staff_id == Staff.staff_id)
    )
    if last_sync:
        q = q.filter(Payment.last_update > last_sync)

    rows = q.all()
    max_ts = last_sync

    for p, st in rows:
        store_key = store_map.get(int(st.store_id))
        customer_key = customer_map.get(int(p.customer_id))
        if store_key is None or customer_key is None:
            continue

        paid_key = make_date_key(p.payment_date)

        existing = sqlite_session.query(FactPayment).filter_by(payment_id=p.payment_id).first()
        if existing:
            existing.date_key_paid = paid_key
            existing.customer_key = customer_key
            existing.store_key = store_key
            existing.staff_id = p.staff_id
            existing.amount = float(p.amount)
        else:
            sqlite_session.add(FactPayment(
                payment_id=p.payment_id,
                date_key_paid=paid_key,
                customer_key=customer_key,
                store_key=store_key,
                staff_id=p.staff_id,
                amount=float(p.amount),
            ))

        if max_ts is None or p.last_update > max_ts:
            max_ts = p.last_update

    if max_ts:
        set_last_sync(sqlite_session, "payment", max_ts)

    sqlite_session.commit()
    print(f"Incremental fact_payment: {len(rows)} rows processed")


def validate(days: int = 30, tolerance: float = 0.01) -> None:
    mysql_session = get_mysql_session()
    sqlite_session = get_sqlite_session()

    try:
        cutoff = datetime.utcnow() - timedelta(days=days)

      
        mysql_rental_count = (
            mysql_session.query(func.count(Rental.rental_id))
            .filter(Rental.rental_date >= cutoff)
            .scalar()
        )

        cutoff_key = int(cutoff.strftime("%Y%m%d"))
        sqlite_rental_count = (
            sqlite_session.query(func.count(FactRental.rental_id))
            .filter(FactRental.date_key_rented >= cutoff_key)
            .scalar()
        )

        print(f"Rentals last {days} days — MySQL: {mysql_rental_count}, SQLite: {sqlite_rental_count}")
        if mysql_rental_count != sqlite_rental_count:
            print("Rental count mismatch")

        
        mysql_payment_total = (
            mysql_session.query(func.coalesce(func.sum(Payment.amount), 0))
            .filter(Payment.payment_date >= cutoff)
            .scalar()
        )
       
        mysql_payment_total = float(mysql_payment_total)

        sqlite_payment_total = (
            sqlite_session.query(func.coalesce(func.sum(FactPayment.amount), 0.0))
            .filter(FactPayment.date_key_paid >= cutoff_key)
            .scalar()
        )

        print(f"Payments last {days} days — MySQL total: {mysql_payment_total:.2f}, SQLite total: {sqlite_payment_total:.2f}")
        if abs(mysql_payment_total - sqlite_payment_total) > tolerance:
            raise RuntimeError("Payment total mismatch beyond tolerance")

        
        mysql_by_store = (
            mysql_session.query(
                Staff.store_id.label("store_id"),
                func.coalesce(func.sum(Payment.amount), 0).label("total")
            )
            .join(Staff, Payment.staff_id == Staff.staff_id)
            .filter(Payment.payment_date >= cutoff)
            .group_by(Staff.store_id)
            .all()
        )
        mysql_by_store = {int(sid): float(total) for sid, total in mysql_by_store}

        sqlite_by_store = (
            sqlite_session.query(
                DimStore.store_id.label("store_id"),
                func.coalesce(func.sum(FactPayment.amount), 0.0).label("total")
            )
            .join(DimStore, FactPayment.store_key == DimStore.store_key)
            .filter(FactPayment.date_key_paid >= cutoff_key)
            .group_by(DimStore.store_id)
            .all()
        )
        sqlite_by_store = {int(sid): float(total) for sid, total in sqlite_by_store}

        all_store_ids = set(mysql_by_store.keys()) | set(sqlite_by_store.keys())
        mismatches = 0
        for sid in sorted(all_store_ids):
            a = mysql_by_store.get(sid, 0.0)
            b = sqlite_by_store.get(sid, 0.0)
            if abs(a - b) > tolerance:
                mismatches += 1
                print(f"Store {sid} payment total mismatch — MySQL {a:.2f} vs SQLite {b:.2f}")

        if mismatches:
            raise RuntimeError(f"{mismatches} store total mismatches beyond tolerance")

        print("Validation passed")

    finally:
        mysql_session.close()
        sqlite_session.close()


def init_db() -> None:
    # 1) Verify MySQL connection (no ORM yet—just connection test)
    mysql_engine = get_mysql_engine()
    with mysql_engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    # 2) Create SQLite tables
    sqlite_engine = get_sqlite_engine()
    BaseSQLite.metadata.create_all(sqlite_engine)

    # 3) Populate dim_date (wide safe range)
    session = get_sqlite_session()
    try:
        populate_dim_date(session, start=date(2000, 1, 1), end=date(2030, 12, 31))

        
        tables = [
            "film","actor","category","store","customer",
            "rental","payment","inventory","staff","language",
            "address","city","country","film_actor","film_category"
            ]

        for name in tables:
            exists = session.query(SyncState).filter_by(table_name=name).first()
            if not exists:
                session.add(SyncState(table_name=name))


    except:
        session.rollback()
        raise
    finally:
        session.close()

