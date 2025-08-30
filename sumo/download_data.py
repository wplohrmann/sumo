import os
import sqlite3
from typing import List, Dict, Any
import logging
from tqdm import tqdm

from sumo.bashos import bashos
from datetime import datetime, timedelta

from sumo.utils import fetch

DB_PATH = os.path.join(os.path.dirname(__file__), "sumo.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def init_db() -> None:
    if not os.path.exists(DB_PATH):
        with open(SCHEMA_PATH) as f:
            schema = f.read()
        conn = sqlite3.connect(DB_PATH)
        conn.executescript(schema)
        conn.close()


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def maybe_insert_basho(conn: sqlite3.Connection, basho_id: str) -> None:
    exists = conn.execute("SELECT 1 FROM basho WHERE id = ?", (basho_id,)).fetchone()
    if exists:
        return
    basho = fetch(f"/basho/{basho_id}")
    conn.execute(
        "INSERT OR IGNORE INTO basho (id, name, start_date, end_date) VALUES (?, ?, ?, ?)",
        (basho_id, basho.get("location"), basho.get("startDate"), basho.get("endDate")),
    )


def maybe_insert_basho_rikishi(
    conn: sqlite3.Connection, basho_id: str, division: str
) -> None:
    exists = conn.execute(
        "SELECT 1 FROM basho_rikishi WHERE basho_id = ? AND division = ?",
        (basho_id, division),
    ).fetchone()
    if exists:
        return
    data = fetch(f"/basho/{basho_id}/banzuke/{division}")
    for rikishi in data.get("rikishi", []):
        conn.execute(
            "INSERT OR IGNORE INTO basho_rikishi (basho_id, rikishi_id, rank, division) VALUES (?, ?, ?, ?)",
            (
                basho_id,
                rikishi.get("id"),
                rikishi.get("rank"),
                division,
            ),
        )


def maybe_insert_rikishi_details(conn: sqlite3.Connection, basho_id: str) -> None:
    rikishi_this_basho = conn.execute(
        "SELECT rikishi_id FROM basho_rikishi WHERE basho_id = ?", (basho_id,)
    ).fetchall()
    already_exists = conn.execute(
        "SELECT id FROM rikishi WHERE basho_id = ?", (basho_id,)
    ).fetchall()
    to_fetch = set(rikishi_this_basho) - set(already_exists)

    for rikishi_id in to_fetch:
        data = fetch(f"/rikishi/{rikishi_id}")
        conn.execute(
            "INSERT OR IGNORE INTO rikishi (id, name, rank, debut_date, birth_date) VALUES (?, ?, ?, ?, ?)",
            (
                data.get("id"),
                data.get("name"),
                data.get("rank"),
                data.get("debut"),
                data.get("birthDate"),
            ),
        )


def maybe_insert_measurements(conn: sqlite3.Connection, basho_id: str) -> None:
    logging.info(f"Fetching measurements for basho {basho_id}")
    data = fetch(f"/measurements?bashoId={basho_id}")
    measurements: List[Dict[str, Any]] = data.get("measurements", [])
    for m in measurements:
        conn.execute(
            "INSERT OR IGNORE INTO measurement (rikishi_id, basho_id, height_cm, weight_kg) VALUES (?, ?, ?, ?)",
            (
                m.get("rikishiId"),
                basho_id,
                m.get("height"),
                m.get("weight"),
            ),
        )
        conn.commit()  # Commit after measurement writes


def maybe_insert_matches(
    conn: sqlite3.Connection, basho_id: str, division: str, day: int
) -> None:
    logging.info(
        f"Fetching matches for basho {basho_id}, division {division}, day {day}"
    )
    row = conn.execute(
        "SELECT start_date FROM basho WHERE id = ?", (basho_id,)
    ).fetchone()
    start_date = datetime.strptime(row[0], "%Y-%m-%d")
    match_data: Dict[str, Any] = fetch(f"/basho/{basho_id}/torikumi/{division}/{day}")
    for match in match_data.get("torikumi", []):
        conn.execute(
            "INSERT OR IGNORE INTO match (id, basho_id, rikishi1_id, rikishi2_id, winner_id, kimarite, day, match_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                match.get("id"),
                basho_id,
                match.get("eastId"),
                match.get("westId"),
                match.get("winnerId"),
                match.get("kimarite"),
                day,
                start_date + timedelta(days=day - 1),
            ),
        )
        conn.commit()


def main(basho_ids: List[str]) -> None:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    divisions: List[str] = [
        "Makuuchi",
        "Juryo",
        "Makushita",
        "Sandanme",
        "Jonidan",
        "Jonokuchi",
    ]
    logging.info(f"Starting to process {len(basho_ids)} bashos...")
    for basho_id in tqdm(basho_ids, desc="Bashos"):
        maybe_insert_basho(conn, basho_id)
        for division in tqdm(divisions, desc=f"Divisions for {basho_id}", leave=False):
            maybe_insert_basho_rikishi(conn, basho_id, division)

        maybe_insert_rikishi_details(conn, basho_id)
        maybe_insert_measurements(conn, basho_id)

        for division in tqdm(
            divisions, desc=f"Match Divisions {basho_id}", leave=False
        ):
            for day in tqdm(range(1, 16), desc=f"Days {division}", leave=False):
                maybe_insert_matches(conn, basho_id, division, day)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    basho_ids = bashos[:1]
    main(basho_ids)
