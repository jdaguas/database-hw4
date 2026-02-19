import os
import sqlite3
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text


def run_cli(cmd: str, env: dict) -> str:
    p = subprocess.run(
        ["python", "app.py", cmd],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return p.stdout + p.stderr


def mysql_engine(env: dict):
    return create_engine(env["MYSQL_URL"], future=True)


def sqlite_conn(db_path: Path):
    return sqlite3.connect(db_path)


def pick_free_category_id(engine) -> int:
    with engine.connect() as c:
        used = {row[0] for row in c.execute(text("SELECT category_id FROM category")).all()}
    for candidate in range(200, 256):
        if candidate not in used:
            return candidate
    raise RuntimeError("No free category_id found in 200..255")

gi
@pytest.fixture
def test_env(tmp_path: Path):
    mysql_url = os.environ.get("MYSQL_URL")
    if not mysql_url:
        pytest.skip("MYSQL_URL is not set; integration tests require MySQL Sakila.")

    db_path = tmp_path / "analytics_sakila_test.db"
    env = os.environ.copy()
    env["MYSQL_URL"] = mysql_url
    env["SQLITE_URL"] = f"sqlite:///{db_path}"
    return env, db_path


def test_init(test_env):
    env, db_path = test_env

    out = run_cli("init", env)
    assert "Init" in out
    assert db_path.exists()

    with sqlite_conn(db_path) as con:
        tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"dim_date", "sync_state", "dim_film", "fact_rental", "fact_payment"} <= tables


def test_full_load(test_env):
    env, db_path = test_env

    run_cli("init", env)
    run_cli("full-load", env)

    with sqlite_conn(db_path) as con:
        dim_film = con.execute("SELECT COUNT(*) FROM dim_film").fetchone()[0]
        dim_actor = con.execute("SELECT COUNT(*) FROM dim_actor").fetchone()[0]
        fact_payment = con.execute("SELECT COUNT(*) FROM fact_payment").fetchone()[0]

    assert dim_film > 0
    assert dim_actor > 0
    assert fact_payment > 0


def test_incremental_new_data(test_env):
    env, db_path = test_env
    engine = mysql_engine(env)

    run_cli("init", env)
    run_cli("full-load", env)

    new_name = "ZZ_TEST_CAT"
    new_id = pick_free_category_id(engine)

    # Arrange: insert into MySQL
    with engine.begin() as c:
        c.execute(
            text("INSERT INTO category (category_id, name, last_update) VALUES (:id, :name, NOW())"),
            {"id": new_id, "name": new_name},
        )

    try:
        # Act
        run_cli("incremental", env)

        # Assert: exists in SQLite
        with sqlite_conn(db_path) as con:
            cnt = con.execute(
                "SELECT COUNT(*) FROM dim_category WHERE category_id = ? AND name = ?",
                (new_id, new_name),
            ).fetchone()[0]
        assert cnt == 1

    finally:
        # Cleanup MySQL
        with engine.begin() as c:
            c.execute(text("DELETE FROM category WHERE category_id = :id"), {"id": new_id})


def test_incremental_updates(test_env):
    env, db_path = test_env
    engine = mysql_engine(env)

    run_cli("init", env)
    run_cli("full-load", env)

    actor_id = 1
    new_last = "ZZ_TEST_LAST"

    
    with engine.connect() as c:
        original_last = c.execute(
            text("SELECT last_name FROM actor WHERE actor_id = :id"),
            {"id": actor_id},
        ).scalar_one()

    with engine.begin() as c:
        c.execute(
            text("UPDATE actor SET last_name = :new_last, last_update = NOW() WHERE actor_id = :id"),
            {"new_last": new_last, "id": actor_id},
        )

    try:
        # Act
        run_cli("incremental", env)

        # Assert
        with sqlite_conn(db_path) as con:
            got = con.execute(
                "SELECT last_name FROM dim_actor WHERE actor_id = ?",
                (actor_id,),
            ).fetchone()
        assert got and got[0] == new_last

    finally:
        
        with engine.begin() as c:
            c.execute(
                text("UPDATE actor SET last_name = :orig, last_update = NOW() WHERE actor_id = :id"),
                {"orig": original_last, "id": actor_id},
            )


def test_validate_command_passes(test_env):
    env, _ = test_env

    run_cli("init", env)
    run_cli("full-load", env)

    out = run_cli("validate", env)
    assert "Validation passed" in out
