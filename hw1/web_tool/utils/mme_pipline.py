# web_tool/utils/mme_pipline.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Tuple, Union, IO, Optional
from io import StringIO
import time, sqlite3, re
import pandas as pd

# 型別：路徑或已開啟的文字檔
LineSource = Union[str, Path, IO[str]]

# ---------- 共用：支援 path 或 file-like ----------
def _iter_lines(src: LineSource) -> Iterable[str]:
    if hasattr(src, "read"):  # file-like
        for line in src:  # type: ignore
            yield line.rstrip("\r\n")
        return
    p = Path(src) if not isinstance(src, Path) else src
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield line.rstrip("\r\n")

def find_common_sqlite(query_src: LineSource, human_src: LineSource, k: int, tmp_db=":memory:") -> pd.DataFrame:
    """
    在 SQLite 內建兩張暫存表 (query_kmers, human_kmers)，建立索引後用 SQL join 取交集。
    適合大型 FASTA，省 RAM。
    """
    conn = sqlite3.connect(tmp_db)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    try:
        # 建表
        conn.execute("""CREATE TABLE query_kmers(
            query_protein_name TEXT, query_protein_length INT,
            kmer_start INT, kmer_end INT, kmer TEXT, k INT
        );""")
        conn.execute("""CREATE TABLE human_kmers(
            hit_human_protein_name TEXT, hit_human_protein_length INT,
            kmer_start INT, kmer_end INT, kmer TEXT, k INT
        );""")

        # 分批插入（避免一次塞爆記憶體）
        def _insert_kmers(cur, table, rows, chunk=50_000):
            buf = []
            for r in rows:
                buf.append(r)
                if len(buf) >= chunk:
                    cur.executemany(
                        f"INSERT INTO {table} VALUES (?,?,?,?,?,?)", buf)
                    buf.clear()
            if buf:
                cur.executemany(
                    f"INSERT INTO {table} VALUES (?,?,?,?,?,?)", buf)

        # 產 query k-mers → insert
        q_rows = (
            (name, L, s+1, s+k, seq[s:s+k], k)
            for name, seq in parse_fasta(query_src)
            for L in (len(seq),)
            for s in range(max(0, L-k+1))
        )
        cur = conn.cursor()
        _insert_kmers(cur, "query_kmers", q_rows)

        # 產 human k-mers → insert
        h_rows = (
            (name, L, s+1, s+k, seq[s:s+k], k)
            for name, seq in parse_fasta(human_src)
            for L in (len(seq),)
            for s in range(max(0, L-k+1))
        )
        _insert_kmers(cur, "human_kmers", h_rows)

        # 索引（關鍵：kmer + k）
        cur.execute("CREATE INDEX IF NOT EXISTS ix_q ON query_kmers(kmer, k);")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_h ON human_kmers(kmer, k);")

        # join
        sql = """
        SELECT
            q.kmer AS "MME(query)",
            h.kmer AS "MME(hit)",
            q.query_protein_name, q.query_protein_length,
            q.k AS "length_of_MME(query)",
            q.kmer_start AS "MME(query)_start", q.kmer_end AS "MME(query)_end",
            h.hit_human_protein_name, h.hit_human_protein_length,
            h.k AS "length_of_MME(hit)",
            h.kmer_start AS "MME(hit)_start", h.kmer_end AS "MME(hit)_end"
        FROM query_kmers q
        JOIN human_kmers h
          ON q.kmer = h.kmer AND q.k = h.k;
        """
        df = pd.read_sql(sql, conn)
        return df
    finally:
        conn.close()

def find_common_ac(query_src: LineSource, human_src: LineSource, k: int) -> pd.DataFrame:
    try:
        import ahocorasick  # pip install pyahocorasick
    except ImportError:
        # 沒裝套件時退回 SQLite 法
        return find_common_sqlite(query_src, human_src, k)

    # 1) 建 AC：只用查詢序列的所有長度 k 的 k-mer
    A = ahocorasick.Automaton()
    # value: (kmer_str, query_protein_name, query_protein_length, start, end)
    for q_name, q_seq in parse_fasta(query_src):
        Lq = len(q_seq)
        for s in range(max(0, Lq - k + 1)):
            km = q_seq[s:s+k]
            # 允許多重鍵，value 用 list
            A.add_word(km, (km, q_name, Lq, s+1, s+k))
    A.make_automaton()

    # 2) 掃人類序列；命中即輸出
    rows = []
    for h_name, h_seq in parse_fasta(human_src):
        Lh = len(h_seq)
        for end_idx, (km, q_name, Lq, qs, qe) in A.iter(h_seq):
            he = end_idx + 1           # 1-based
            hs = he - k + 1
            rows.append({
                "MME(query)": km, "MME(hit)": km,
                "query_protein_name": q_name, "query_protein_length": Lq,
                "length_of_MME(query)": k, "MME(query)_start": qs, "MME(query)_end": qe,
                "hit_human_protein_name": h_name, "hit_human_protein_length": Lh,
                "length_of_MME(hit)": k, "MME(hit)_start": hs, "MME(hit)_end": he,
            })
    return pd.DataFrame(rows)
# ---------- 1) FASTA 讀取 + 產生 k-mer ----------
def parse_fasta(src: LineSource) -> Iterable[Tuple[str, str]]:
    name: Optional[str] = None
    seq_chunks: list[str] = []
    for s in _iter_lines(src):
        if not s:
            continue
        if s.startswith(">"):
            if name is not None:
                yield name, re.sub(r"[^A-Z]", "", "".join(seq_chunks).upper())
            name = s[1:].strip()
            seq_chunks.clear()
        else:
            seq_chunks.append(s.strip())
    if name is not None:
        yield name, re.sub(r"[^A-Z]", "", "".join(seq_chunks).upper())

def kmers_df(src: LineSource, k: int) -> pd.DataFrame:
    rows = []
    for name, seq in parse_fasta(src):
        L = len(seq)
        n = L - k + 1
        if n <= 0: 
            continue
        # list append 其實已經很快；若要再更快可考慮 numpy slicing，但可讀性較差
        for s in range(n):
            rows.append((name, L, s+1, s+k, seq[s:s+k], k))
    df = pd.DataFrame.from_records(
        rows,
        columns=["protein_name","protein_length","kmer_start","kmer_end","kmer","k"],
    )
    # 設 dtype
    for c in ("protein_length","kmer_start","kmer_end","k"):
        df[c] = df[c].astype("int32", copy=False)
    return df

# ---------- 2) 等值連接找共通 k-mer ----------
def find_common_df(query_df: pd.DataFrame, human_df: pd.DataFrame) -> pd.DataFrame:
    q = query_df.rename(columns={
        "protein_name": "query_protein_name",
        "protein_length": "query_protein_length",
        "kmer_start": "MME(query)_start",
        "kmer_end":   "MME(query)_end",
    })
    h = human_df.rename(columns={
        "protein_name": "hit_human_protein_name",
        "protein_length": "hit_human_protein_length",
        "kmer_start": "MME(hit)_start",
        "kmer_end":   "MME(hit)_end",
    })
    m = q.merge(h, on=["kmer", "k"], how="inner", copy=False)
    m["MME(query)"] = m["kmer"]
    m["MME(hit)"]   = m["kmer"]
    m["length_of_MME(query)"] = m["k"]
    m["length_of_MME(hit)"]   = m["k"]
    m = m.drop(columns=["k"])
    m = m[[
        "MME(query)", "MME(hit)",
        "query_protein_name", "query_protein_length",
        "length_of_MME(query)", "MME(query)_start", "MME(query)_end",
        "hit_human_protein_name", "hit_human_protein_length",
        "length_of_MME(hit)", "MME(hit)_start", "MME(hit)_end",
    ]].copy()
    return m

# ---------- 3) 將連續 (+1,+1) 的 match 串接 ----------
def stitch_consecutive(common_df: pd.DataFrame) -> pd.DataFrame:
    df = common_df.sort_values(
        by=["hit_human_protein_name", "query_protein_name", "MME(hit)_start", "MME(query)_start"],
        kind="mergesort",
    ).reset_index(drop=True)

    same_pair = (
        df["hit_human_protein_name"].eq(df["hit_human_protein_name"].shift())
        & df["query_protein_name"].eq(df["query_protein_name"].shift())
    )
    is_break = ~(same_pair & df["MME(hit)_start"].diff().eq(1) & df["MME(query)_start"].diff().eq(1))
    group_id = is_break.cumsum()

    out = []
    for _, g in df.groupby(group_id, sort=False):
        first = g.iloc[0]
        if len(g) == 1:
            length = int(first.get("length_of_MME(query)", 1))
            out.append({
                "MME(query)": first["MME(query)"],
                "MME(hit)": first["MME(hit)"],
                "query_protein_name": first["query_protein_name"],
                "query_protein_length": int(first["query_protein_length"]),
                "length_of_MME(query)": length,
                "MME(query)_start": int(first["MME(query)_start"]),
                "MME(query)_end": int(first["MME(query)_end"]),
                "hit_human_protein_name": first["hit_human_protein_name"],
                "hit_human_protein_length": int(first["hit_human_protein_length"]),
                "length_of_MME(hit)": length,
                "MME(hit)_start": int(first["MME(hit)_start"]),
                "MME(hit)_end": int(first["MME(hit)_end"]),
            })
            continue

        kmer0 = first["MME(query)"]
        merged = kmer0 + "".join(mm[-1] for mm in g["MME(query)"].iloc[1:])
        length = len(merged)
        qs = int(first["MME(query)_start"])
        hs = int(first["MME(hit)_start"])
        out.append({
            "MME(query)": merged, "MME(hit)": merged,
            "query_protein_name": first["query_protein_name"],
            "query_protein_length": int(first["query_protein_length"]),
            "length_of_MME(query)": length, "MME(query)_start": qs, "MME(query)_end": qs + length - 1,
            "hit_human_protein_name": first["hit_human_protein_name"],
            "hit_human_protein_length": int(first["hit_human_protein_length"]),
            "length_of_MME(hit)": length, "MME(hit)_start": hs, "MME(hit)_end": hs + length - 1,
        })

    out_df = pd.DataFrame(out).sort_values(
        by=["MME(query)", "MME(hit)_start"], kind="mergesort"
    ).reset_index(drop=True)
    return out_df

# ---------- 4) 一條龍：完全不落地 ----------
def run_pipeline(query_src: LineSource, human_src: LineSource, k: int = 6, backend: str = "auto") -> pd.DataFrame:
    assert isinstance(k, int) and k > 0, "k 必須是正整數"
    t0 = time.time()

    if backend == "ac":
        common = find_common_ac(query_src, human_src, k)
    elif backend == "sqlite":
        common = find_common_sqlite(query_src, human_src, k)
    else:  # auto：小資料走 pandas、否則退 sqlite
        # 粗略估計：若 query 與 human 皆不大，可以用既有邏輯
        try:
            q_df = kmers_df(query_src, k)
            h_df = kmers_df(human_src, k)
            common = find_common_df(q_df, h_df)
        except MemoryError:
            common = find_common_sqlite(query_src, human_src, k)

    stitched = stitch_consecutive(common)
    stitched.attrs["elapsed_sec"] = time.time() - t0
    return stitched

# ---------- 輔助：DataFrame -> JSON / CSV ----------
def df_to_records(df: pd.DataFrame, limit: Optional[int] = None):
    if limit is not None:
        df = df.head(limit)
    return df.to_dict(orient="records")

def df_to_csv_text(df: pd.DataFrame) -> str:
    buf = StringIO(); df.to_csv(buf, index=False)
    return buf.getvalue()

# ---------- 路徑 / file-like ----------
def run_from_paths(query_path: str, human_path: str, k: int = 6) -> pd.DataFrame:
    t0 = time.time()
    df = run_pipeline(query_path, human_path, k)
    df.attrs["elapsed_sec"] = time.time() - t0
    return df

def run_from_files(query_file: IO[str], human_file: IO[str], k: int = 6) -> pd.DataFrame:
    t0 = time.time()
    df = run_pipeline(query_file, human_file, k)
    df.attrs["elapsed_sec"] = time.time() - t0
    return df

# ---------- 存到 SQLite（snake_case 欄名） ----------
def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [re.sub(r'[^0-9a-zA-Z_]+', '_', c).strip('_').lower() for c in out.columns]
    return out

def save_append(df: pd.DataFrame, db_path="results.sqlite3", table="mme_result", chunksize=50_000) -> int:
    t0 = time.time()
    sdf = _sanitize_columns(df)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        sdf.to_sql(table, conn, if_exists="append", index=False, chunksize=chunksize, method="multi")
        # 可選：建立常用索引（只建立一次即可；失敗忽略）
        try:
            conn.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_kmer_q ON {table}("mme_query");')
            conn.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_kmer_h ON {table}("mme_hit");')
        except Exception:
            pass
    added = int(len(sdf))
    df.attrs["save_elapsed_sec"] = time.time() - t0
    return added

if __name__ == "__main__":
    pass