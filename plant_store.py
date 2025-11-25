"""
Lightweight SQLite-backed registry for plant metadata.

Stores:
  - alias (human-friendly key)
  - plant_uid
  - inverter_ids (JSON array of EMIG IDs)
  - weather_id (optional)
"""

import json
import os
import sqlite3
from typing import Dict, List, Optional, Sequence


DEFAULT_DB = os.path.join(os.path.dirname(__file__), "plant_registry.sqlite")


class PlantStore:
    def __init__(self, db_path: str = DEFAULT_DB) -> None:
        self.db_path = db_path
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS plants (
                    alias TEXT PRIMARY KEY,
                    plant_uid TEXT NOT NULL,
                    inverter_ids TEXT NOT NULL,
                    weather_id TEXT,
                    dc_size_kw REAL
                )
                """
            )
            # Backward compatibility: add dc_size_kw if missing
            try:
                conn.execute("ALTER TABLE plants ADD COLUMN dc_size_kw REAL")
            except Exception:
                pass
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fetch_cache (
                    plant_uid TEXT NOT NULL,
                    emig_id TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (plant_uid, emig_id, start_date, end_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS readings (
                    plant_uid TEXT NOT NULL,
                    emig_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (plant_uid, emig_id, ts)
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def save(self, alias: str, plant_uid: str, inverter_ids: List[str], weather_id: Optional[str], dc_size_kw: Optional[float] = None) -> None:
        payload = json.dumps(inverter_ids)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                INSERT INTO plants (alias, plant_uid, inverter_ids, weather_id, dc_size_kw)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(alias) DO UPDATE SET
                    plant_uid=excluded.plant_uid,
                    inverter_ids=excluded.inverter_ids,
                    weather_id=excluded.weather_id,
                    dc_size_kw=excluded.dc_size_kw
                """,
                (alias, plant_uid, payload, weather_id, dc_size_kw),
            )
            conn.commit()
        finally:
            conn.close()

    def load(self, alias: str) -> Optional[Dict]:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                "SELECT plant_uid, inverter_ids, weather_id, dc_size_kw FROM plants WHERE alias = ?",
                (alias,),
            )
            row = cur.fetchone()
            if not row:
                return None
            plant_uid, inverter_ids_json, weather_id, dc_size_kw = row
            return {
                "alias": alias,
                "plant_uid": plant_uid,
                "inverter_ids": json.loads(inverter_ids_json),
                "weather_id": weather_id,
                "dc_size_kw": dc_size_kw,
            }
        finally:
            conn.close()

    def list_all(self) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute("SELECT alias, plant_uid, weather_id, dc_size_kw FROM plants ORDER BY alias")
            return [
                {"alias": alias, "plant_uid": plant_uid, "weather_id": weather_id, "dc_size_kw": dc_size_kw}
                for alias, plant_uid, weather_id, dc_size_kw in cur.fetchall()
            ]
        finally:
            conn.close()

    def first(self) -> Optional[Dict]:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute("SELECT alias, plant_uid, inverter_ids, weather_id, dc_size_kw FROM plants ORDER BY alias LIMIT 1")
            row = cur.fetchone()
            if not row:
                return None
            alias, plant_uid, inverter_ids_json, weather_id, dc_size_kw = row
            return {
                "alias": alias,
                "plant_uid": plant_uid,
                "inverter_ids": json.loads(inverter_ids_json),
                "weather_id": weather_id,
                "dc_size_kw": dc_size_kw,
            }
        finally:
            conn.close()

    def alias_for(self, plant_uid: str) -> Optional[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute("SELECT alias FROM plants WHERE plant_uid = ? LIMIT 1", (plant_uid,))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def delete(self, alias: str) -> bool:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute("DELETE FROM plants WHERE alias = ?", (alias,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def export_all(self) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute("SELECT alias, plant_uid, inverter_ids, weather_id FROM plants ORDER BY alias")
            rows = cur.fetchall()
            out: List[Dict] = []
            for alias, plant_uid, inverter_ids_json, weather_id in rows:
                out.append(
                    {
                        "alias": alias,
                        "plant_uid": plant_uid,
                        "inverter_ids": json.loads(inverter_ids_json),
                        "weather_id": weather_id,
                    }
                )
            return out
        finally:
            conn.close()

    def import_many(self, records: List[Dict]) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            for rec in records:
                alias = rec["alias"]
                plant_uid = rec["plant_uid"]
                inverter_ids = rec.get("inverter_ids", [])
                weather_id = rec.get("weather_id")
                dc_size_kw = rec.get("dc_size_kw")
                payload = json.dumps(inverter_ids)
                conn.execute(
                    """
                    INSERT INTO plants (alias, plant_uid, inverter_ids, weather_id, dc_size_kw)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(alias) DO UPDATE SET
                        plant_uid=excluded.plant_uid,
                        inverter_ids=excluded.inverter_ids,
                        weather_id=excluded.weather_id,
                        dc_size_kw=excluded.dc_size_kw
                    """,
                    (alias, plant_uid, payload, weather_id, dc_size_kw),
                )
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Fetch cache helpers
    # ------------------------------------------------------------------
    def has_fetch(self, plant_uid: str, emig_id: str, start_date: str, end_date: str) -> bool:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                """
                SELECT 1 FROM fetch_cache
                WHERE plant_uid = ? AND emig_id = ? AND start_date = ? AND end_date = ?
                """,
                (plant_uid, emig_id, start_date, end_date),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()

    def record_fetch(self, plant_uid: str, emig_id: str, start_date: str, end_date: str) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO fetch_cache (plant_uid, emig_id, start_date, end_date)
                VALUES (?, ?, ?, ?)
                """,
                (plant_uid, emig_id, start_date, end_date),
            )
            conn.commit()
        finally:
            conn.close()

    def list_emig_ids(self, plant_uid: str) -> List[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                "SELECT DISTINCT emig_id FROM readings WHERE plant_uid = ? ORDER BY emig_id",
                (plant_uid,),
            )
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def season_range(self, plant_uid: str, months: Sequence[str]) -> Optional[Dict[str, str]]:
        """
        Find a date range for the most recent year that has data in the given month set.
        months: list like ["06","07","08"]
        Returns dict with start_date/end_date (YYYYMMDD) or None.
        """
        if not months:
            return None
        placeholders = ",".join("?" for _ in months)
        sql = f"""
            SELECT substr(ts,1,4) as year,
                   MIN(substr(ts,1,10)) as start_date,
                   MAX(substr(ts,1,10)) as end_date,
                   COUNT(*) as n
            FROM readings
            WHERE plant_uid = ?
              AND substr(ts,6,2) IN ({placeholders})
            GROUP BY year
            ORDER BY year DESC
            LIMIT 1
        """
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(sql, [plant_uid, *months])
            row = cur.fetchone()
            if not row:
                return None
            year, start_date, end_date, _ = row
            start_date = start_date.replace("-", "")
            end_date = end_date.replace("-", "")
            return {"start": start_date, "end": end_date, "year": year}
        finally:
            conn.close()

    def date_span(self, plant_uid: str) -> Optional[Dict[str, str]]:
        """Return min/max ts for a plant."""
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                """
                SELECT MIN(ts), MAX(ts) FROM readings WHERE plant_uid = ?
                """,
                (plant_uid,),
            )
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return {"min": row[0], "max": row[1]}
        finally:
            conn.close()

    def emig_date_spans(self, plant_uid: str) -> List[Dict[str, str]]:
        """Return min/max ts per emig for a plant."""
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                """
                SELECT emig_id, MIN(ts), MAX(ts)
                FROM readings
                WHERE plant_uid = ?
                GROUP BY emig_id
                ORDER BY emig_id
                """,
                (plant_uid,),
            )
            rows = cur.fetchall()
            return [{"emig_id": emig, "min": mn, "max": mx} for emig, mn, mx in rows]
        finally:
            conn.close()

    def store_readings(self, plant_uid: str, emig_id: str, readings: List[Dict]) -> None:
        if not readings:
            return
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executemany(
                """
                INSERT OR REPLACE INTO readings (plant_uid, emig_id, ts, payload)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (plant_uid, emig_id, r.get("ts"), json.dumps(r))
                    for r in readings
                    if r.get("ts") is not None
                ],
            )
            conn.commit()
        finally:
            conn.close()

    def load_readings(self, plant_uid: str, emig_id: str, start_ts: str, end_ts: str) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                """
                SELECT payload FROM readings
                WHERE plant_uid = ? AND emig_id = ? AND ts >= ? AND ts <= ?
                ORDER BY ts
                """,
                (plant_uid, emig_id, start_ts, end_ts),
            )
            return [json.loads(row[0]) for row in cur.fetchall()]
        finally:
            conn.close()

    def delete_device_readings(self, plant_uid: str, emig_id: str) -> int:
        """Delete all readings for a specific device. Returns number of rows deleted."""
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                """
                DELETE FROM readings
                WHERE plant_uid = ? AND emig_id = ?
                """,
                (plant_uid, emig_id),
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def delete_devices_by_pattern(self, plant_uid: str, pattern: str) -> int:
        """Delete all readings for devices matching a pattern (e.g., 'POA:%', 'WETH:%'). Returns number of rows deleted."""
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(
                """
                DELETE FROM readings
                WHERE plant_uid = ? AND emig_id LIKE ?
                """,
                (plant_uid, pattern),
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()
