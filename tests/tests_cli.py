import os
import re
import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime

import pytest


def run_cli(cmd: str, env: dict) -> str:
    """
    Run: python app.py <cmd> with env vars.
    Returns stdout as a string. Raises if command fails.
    """
    p = subprocess.run(
        ["python", "app.py", cmd],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return p.stdout + p.stderr


@pytest.fixture
def test_env(tmp_path: Path):
    """
    Provide env vars so tests use a temp SQLite file and your MYSQL_URL.
    """
    mysql_url = os.environ.get("MYSQL_URL")
    if not mysql_url:
        pytest.skip("MYSQL_URL is not set in the environment; can't run integration tests.")

    db_path = tmp_path / "analytics_sakila_test.db"
    env = os.environ.copy()
    env["MYSQL_URL"] = mysql_url
    env["SQLITE_URL"] = f"sqlite:///{db_path}"
    return env, db_path


def test_init_creates_sqlite_and_tables(test_env):
    env, db_path = test_env

    out = run_cli("init", env)
    assert "Init" in out  # your CLI prints a success message

    assert db_path.exists(), "SQLite db file was not created"

    con = sqlite3.connect(db_path)
    try:
        tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        # core tables you must have
        assert "dim_date" in tables
        assert "sync_state" in tables
        assert "dim_film" in tables
        assert "fact_rental" in tables
        assert "fact_payment" in tables
    finally:
        con.close()


def test_full_load_populates_data(test_env):
    env, db_path = test_env

    run_cli("init", env)
    run_cli("full-load", env)

    con = sqlite3.connect(db_path)
    try:
        dim_film = con.execute("SELECT COUNT(*) FROM dim_film").fetchone()[0]
        dim_actor = con.execute("SELECT COUNT(*) FROM dim_actor").fetchone()[0]
        fact_payment = con.execute("SELECT COUNT(*) FROM fact_payment").fetchone()[0]

        assert dim_film > 0
        assert dim_actor > 0
        assert fact_payment > 0
    finally:
        con.close()


def test_incremental_new_data_category_appears_in_sqlite(test_env):
    """
    Insert a new Category row in MySQL (safe + minimal), run incremental,
    verify it appears in SQLite dim_category.
    """
    env, db_path = test_env

    run_cli("init", env)
    run_cli("full-load", env)

    new_name = "ZZ_TEST_CAT"

    # Pick an unused category_id in the valid range (often TINYINT UNSIGNED: 1..255)
    snippet_pick_id = r"""
import os
from sqlalchemy import create_engine, text

e = create_engine(os.environ["MYSQL_URL"])
with e.connect() as c:
    used = {row[0] for row in c.execute(text("SELECT category_id FROM category")).fetchall()}

# Try near the top of the range to avoid collisions with existing Sakila ids
for candidate in range(200, 256):
    if candidate not in used:
        print(candidate)
        break
else:
    raise SystemExit("No free category_id found in 200..255")
"""
    new_id = int(
        subprocess.run(
            ["python", "-c", snippet_pick_id],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )

    insert_sql = f"""
    INSERT INTO category (category_id, name, last_update)
    VALUES ({new_id}, '{new_name}', NOW());
    """
    cleanup_sql = f"DELETE FROM category WHERE category_id = {new_id};"

    snippet_insert = f"""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["MYSQL_URL"])
with e.begin() as c:
    c.execute(text(\"\"\"{insert_sql}\"\"\"))
"""
    subprocess.run(["python", "-c", snippet_insert], env=env, check=True)

    try:
        run_cli("incremental", env)

        con = sqlite3.connect(db_path)
        try:
            cnt = con.execute(
                "SELECT COUNT(*) FROM dim_category WHERE category_id = ? AND name = ?",
                (new_id, new_name),
            ).fetchone()[0]
            assert cnt == 1
        finally:
            con.close()

    finally:
        snippet_cleanup = f"""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["MYSQL_URL"])
with e.begin() as c:
    c.execute(text(\"\"\"{cleanup_sql}\"\"\"))
"""
        subprocess.run(["python", "-c", snippet_cleanup], env=env, check=True)


def test_incremental_updates_actor_is_updated_in_sqlite(test_env):
    """
    Update an existing Actor in MySQL, run incremental,
    verify dim_actor updates in SQLite, then revert.
    """
    env, db_path = test_env

    run_cli("init", env)
    run_cli("full-load", env)

    actor_id = 1
    new_last = "ZZ_TEST_LAST"

    # Read original last_name from MySQL, then update, then revert
    snippet_read = f"""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["MYSQL_URL"])
with e.connect() as c:
    v = c.execute(text("SELECT last_name FROM actor WHERE actor_id = {actor_id}")).scalar()
    print(v)
"""
    original_last = subprocess.run(["python", "-c", snippet_read], env=env, check=True, capture_output=True, text=True).stdout.strip()

    snippet_update = f"""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["MYSQL_URL"])
with e.begin() as c:
    c.execute(text("UPDATE actor SET last_name = '{new_last}', last_update = NOW() WHERE actor_id = {actor_id}"))
"""
    snippet_revert = f"""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["MYSQL_URL"])
with e.begin() as c:
    c.execute(text("UPDATE actor SET last_name = '{original_last}', last_update = NOW() WHERE actor_id = {actor_id}"))
"""

    subprocess.run(["python", "-c", snippet_update], env=env, check=True)

    try:
        run_cli("incremental", env)

        con = sqlite3.connect(db_path)
        try:
            got = con.execute(
                "SELECT last_name FROM dim_actor WHERE actor_id = ?",
                (actor_id,),
            ).fetchone()
            assert got and got[0] == new_last
        finally:
            con.close()
    finally:
        subprocess.run(["python", "-c", snippet_revert], env=env, check=True)


def test_validate_command_passes(test_env):
    env, _ = test_env

    run_cli("init", env)
    run_cli("full-load", env)

    out = run_cli("validate", env)
    assert "Validation passed" in out
