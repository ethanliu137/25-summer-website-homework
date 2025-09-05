# IEDB_pipline.py
from __future__ import annotations
from pathlib import Path
import sqlite3
import re
import pandas as pd
import numpy as np

# ====== 常數：資料庫與檔案路徑 ======
DB_PATH   = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\db.sqlite3"
SRC_TABLE = "mme_result"
DST_TABLE = "iedb_result"
IEDB_CSV  = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\web_tool\static\web_tool\ref\IEDB_human_correct.csv"

# ====== 欄位名稱（和你的 IEDB/ MME 一致）======
COL_EPI      = "MME(query)"
COL_HIT_NAME = "hit_human_protein_name"
COL_HIT_ID   = "hit_human_protein_id"
COL_QNAME    = "query_protein_name"
COL_NAME     = "Name"          # IEDB epitope 名稱
COL_UID      = "UniProt_ID"    # IEDB UniProt 欄
COL_S        = "Starting Position"
COL_E        = "Ending Position"

# 產出欄位
COL_SUBSTR = "IEDB_human_epitope_substring_count"
COL_DATAC  = "IEDB_human_protein_data_count"
COL_FULLY  = "IEDB_human_positional_fully_contained"
COL_PART   = "IEDB_human_positional_partial_overlap"


# ---------- 小工具（供 process 使用） ----------
def _normalize_uniprot(series: pd.Series) -> pd.Series:
    """'sp|P12345|NAME' -> 'P12345'；若沒有 '|' 就原樣回傳"""
    s = series.astype(str).str.strip()
    core = s.str.split("|", n=2).str.get(1)          # 沒有第2欄會得到 NaN
    core = core.where(core.notna() & (core != ""), s)
    return core.str.strip()

def _count_epitope_contains(iedb_names: pd.Series, epitopes: np.ndarray) -> dict[str, int]:
    """
    回傳 {epitope: IEDB Name 欄位中包含它的列數}
    若環境有 pyahocorasick 就用 Aho-Corasick，否則退回逐一 contains。
    """
    try:
        import ahocorasick  # pip install pyahocorasick
        A = ahocorasick.Automaton()
        for idx, pat in enumerate(epitopes):
            if pat:
                A.add_word(pat, (idx, pat))
        A.make_automaton()

        counts = np.zeros(len(epitopes), dtype=np.int64)
        for name in iedb_names.astype(str):
            if not name:
                continue
            hit_once = set()
            for _, (idx, _pat) in A.iter(name):
                hit_once.add(idx)
            if hit_once:
                idxs = np.fromiter(hit_once, dtype=np.int64)
                counts[idxs] += 1
        return {epi: int(c) for epi, c in zip(epitopes, counts)}
    except Exception:
        names = iedb_names.astype(str)
        out: dict[str, int] = {}
        for epi in epitopes:
            out[epi] = int(names.str.contains(epi, regex=False).sum()) if epi else 0
        return out


# ---------- IEDB 核心運算 ----------
def process(match_df: pd.DataFrame, iedb_df: pd.DataFrame) -> pd.DataFrame:
    """
    傳入：MME 結果 DataFrame（含 MME(query)、MME(hit)_start/_end、hit_human_protein_name 等）
          與 IEDB CSV 的 DataFrame
    回傳：在 match_df 上加入 4 個 IEDB 指標欄位
    """
    # (1) 正規化 MME 欄位
    if COL_HIT_ID not in match_df.columns:
        sname = match_df[COL_HIT_NAME].astype(str).str.strip()
        core  = sname.str.split("|", n=2).str.get(1)
        match_df[COL_HIT_ID] = core.where(core.notna() & (core != ""), sname).str.strip()

    match_df[COL_EPI]    = match_df[COL_EPI].astype(str).str.strip().str.upper()
    match_df[COL_HIT_ID] = match_df[COL_HIT_ID].astype(str).str.strip()
    match_df[COL_QNAME]  = match_df[COL_QNAME].astype(str).str.strip()

    # (2) IEDB 清理
    iedb_df = iedb_df.copy()
    iedb_df["_UID_CORE"] = _normalize_uniprot(iedb_df[COL_UID])
    iedb_names = iedb_df[COL_NAME].astype(str)

    # (3) epitope substring 計數
    unique_epi = pd.Index(match_df[COL_EPI].unique())
    epi2cnt = _count_epitope_contains(iedb_names, unique_epi.values)
    match_df[COL_SUBSTR] = match_df[COL_EPI].map(epi2cnt).fillna(0).astype(int)

    # (4) human_protein_data_count（依 UniProt 匹配）
    uid_counts = iedb_df["_UID_CORE"].value_counts()
    match_df["_HIT_CORE"] = _normalize_uniprot(match_df[COL_HIT_ID])
    match_df[COL_DATAC] = match_df["_HIT_CORE"].map(uid_counts).fillna(0).astype(int)

    # (5) fully / partial overlap（以座標廣播）
    iedb_groups: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    ss_all = pd.to_numeric(iedb_df[COL_S], errors="coerce")
    ee_all = pd.to_numeric(iedb_df[COL_E], errors="coerce")
    for uid, grp_idx in iedb_df.groupby("_UID_CORE", sort=False).groups.items():
        S = ss_all.iloc[grp_idx].to_numpy(dtype=float, copy=False)
        E = ee_all.iloc[grp_idx].to_numpy(dtype=float, copy=False)
        mask = ~np.isnan(S) & ~np.isnan(E)
        iedb_groups[str(uid)] = (S[mask], E[mask])

    s_all = pd.to_numeric(match_df["MME(hit)_start"], errors="coerce").to_numpy(dtype=float, copy=False)
    e_all = pd.to_numeric(match_df["MME(hit)_end"],   errors="coerce").to_numpy(dtype=float, copy=False)
    u_all = match_df["_HIT_CORE"].to_numpy(copy=False)

    fully = np.zeros(len(match_df), dtype=np.int32)
    part  = np.zeros(len(match_df), dtype=np.int32)

    for uid, idx in pd.Series(range(len(match_df))).groupby(u_all).groups.items():
        idx = np.fromiter(idx, dtype=np.int64)
        s = s_all[idx]; e = e_all[idx]
        valid = ~(np.isnan(s) | np.isnan(e))
        if not np.any(valid):
            continue
        s = s[valid]; e = e[valid]
        tgt_idx = idx[valid]

        S, E = iedb_groups.get(str(uid), (None, None))
        if S is None or S.size == 0:
            continue

        fully_mat   = (S[:, None] <= s[None, :]) & (E[:, None] >= e[None, :])
        overlap_mat = ~((E[:, None] < s[None, :]) | (S[:, None] > e[None, :]))

        fully[tgt_idx] = fully_mat.sum(axis=0, dtype=np.int32)
        part[tgt_idx]  = overlap_mat.sum(axis=0, dtype=np.int32)

    match_df[COL_FULLY] = fully
    match_df[COL_PART]  = part
    return match_df.drop(columns=["_HIT_CORE"], errors="ignore")


# ---- 讀取 SQLite → 還原欄位名稱（snake_case -> IEDB 預期格式）----
def load_mme_for_iedb(db_path: str = DB_PATH, table: str = SRC_TABLE, limit: int | None = None) -> pd.DataFrame:
    ren_map = {
        "mme_query": "MME(query)",
        "mme_hit": "MME(hit)",
        "query_protein_name": "query_protein_name",
        "query_protein_length": "query_protein_length",
        "length_of_mme_query": "length_of_MME(query)",
        "mme_query_start": "MME(query)_start",
        "mme_query_end": "MME(query)_end",
        "hit_human_protein_name": "hit_human_protein_name",
        "hit_human_protein_length": "hit_human_protein_length",
        "length_of_mme_hit": "length_of_MME(hit)",
        "mme_hit_start": "MME(hit)_start",
        "mme_hit_end": "MME(hit)_end",
        "hit_human_protein_id": "hit_human_protein_id",
    }

    sql = f'SELECT * FROM "{table}"'
    if limit:
        sql += f" LIMIT {int(limit)}"

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    df = df.rename(columns={k: v for k, v in ren_map.items() if k in df.columns})
    return df


# ---- 存回 SQLite（再 sanitize 成 snake_case）----
def save_iedb_back_to_sqlite(df: pd.DataFrame, db_path: str = DB_PATH, table: str = DST_TABLE) -> int:
    def sanitize_columns(d: pd.DataFrame) -> pd.DataFrame:
        out = d.copy()
        out.columns = [re.sub(r'[^0-9a-zA-Z_]+', '_', c).strip('_').lower() for c in out.columns]
        return out

    sdf = sanitize_columns(df)

    conn = sqlite3.connect(db_path)
    try:
        with conn:
            sdf.to_sql(table, conn, if_exists="append", index=False)
            total = pd.read_sql(f'SELECT COUNT(*) AS cnt FROM "{table}"', conn)["cnt"][0]
    finally:
        conn.close()

    print(f"✅ IEDB enriched 已寫回 SQLite → 表 '{table}'，目前 {total} 筆")
    return int(total)


# ---- 一條龍：讀 → enrich → 存 ----
def run_iedb_from_sqlite(
    src_table: str = SRC_TABLE,
    iedb_csv: str | Path = IEDB_CSV,
    dst_table: str = DST_TABLE,
    db_path: str = DB_PATH,
    limit: int | None = None,
) -> pd.DataFrame:
    match_df = load_mme_for_iedb(db_path=db_path, table=src_table, limit=limit)
    iedb_df  = pd.read_csv(iedb_csv, encoding="utf-8-sig")
    enriched = process(match_df, iedb_df)
    save_iedb_back_to_sqlite(enriched, db_path=db_path, table=dst_table)
    return enriched


if __name__ == "__main__":
    run_iedb_from_sqlite()