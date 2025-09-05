# -*- coding: utf-8 -*-
import sqlite3
from pathlib import Path
import pandas as pd

# 直接沿用你原本在 views 裡設定的常數
DB_PATH   = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\iedb_result.sqlite3"
TABLE_ENR = "iedb_result"

def build_summary_by_query(filter_query: str | None = None, limit: int | None = None) -> pd.DataFrame:
    """
    把 iedb_result 先壓成一行一配對 (query_protein_name, hit_human_protein_id)，
    再做外層統計，避免 COUNT(*) 因為展開列而暴增。
    """
    cte = f"""
    WITH base AS (
      SELECT DISTINCT
        TRIM(COALESCE(query_protein_name,''))   AS query_protein_name,
        TRIM(COALESCE(hit_human_protein_id,'')) AS hit_human_protein_id
      FROM "{TABLE_ENR}"
      -- 若你的 CSV 有先過濾（例如 perfect match），請把條件加回來：
      -- WHERE match_type = 'perfect'
    )
    """

    where = ""
    params: list[object] = []
    if filter_query:
        where = " WHERE query_protein_name = ? "
        params.append(filter_query.strip())

    limit_clause = ""
    if isinstance(limit, int) and limit > 0:
        limit_clause = " LIMIT ? "
        params.append(limit)

    sql = cte + f"""
    SELECT
      query_protein_name,
      COUNT(*) AS hit_human_protein_id_kind,                       -- 現在 = 配對數
      COUNT(DISTINCT hit_human_protein_id) AS hit_human_protein_id_sequence_count
    FROM base
    {where}
    GROUP BY query_protein_name
    ORDER BY hit_human_protein_id_kind DESC, query_protein_name ASC
    {limit_clause}
    """

    print("[build_summary_by_query] DB:", DB_PATH, "exists:", Path(DB_PATH).exists())
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(sql, conn, params=params)
    return df