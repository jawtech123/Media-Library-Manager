from __future__ import annotations
import sqlite3
from typing import List, Tuple

from . import db as dbm


def get_duplicates(conn: sqlite3.Connection) -> List[Tuple[int, str, int]]:
    return dbm.fetch_duplicates_by_fullhash(conn)
