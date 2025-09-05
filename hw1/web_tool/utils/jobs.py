import sqlite3, uuid, json, datetime

DB_PATH = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\iedb_result.sqlite3"

def utc_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def ensure_jobs_schema():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        # 1) 建表（若不存在）
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
            -- 其餘欄位等會用 ALTER TABLE 動態補
        )
        """)
        # 2) 檢查既有欄位
        cols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
        def add_col(name, typ):
            if name not in cols:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {typ}")
                cols.add(name)

        # 需要的欄位逐一補
        add_col("short_id",   "TEXT")
        add_col("started_at", "TEXT")
        add_col("finished_at","TEXT")
        add_col("params_json","TEXT")
        add_col("message",    "TEXT")

        # 3) 建唯一索引（避免 short_id 重複）
        idx = [r[1] for r in conn.execute("PRAGMA index_list('jobs')")]
        if "idx_jobs_short_id" not in idx:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_short_id ON jobs(short_id)")

def ensure_job_artifacts_schema():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS job_artifacts (
            job_id TEXT PRIMARY KEY,
            iedb_table TEXT,
            row_count INTEGER,
            created_at TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
        )
        """)

def _gen_short_id(conn) -> str:
    while True:
        cand = uuid.uuid4().hex[:8]
        row = conn.execute("SELECT 1 FROM jobs WHERE short_id=?", (cand,)).fetchone()
        if not row:
            return cand

def create_job(params: dict) -> dict:
    ensure_jobs_schema()
    ensure_job_artifacts_schema()
    job_uuid = str(uuid.uuid4())
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        short_id = _gen_short_id(conn)
        conn.execute("""
            INSERT INTO jobs (job_id, short_id, status, created_at, params_json)
            VALUES (?, ?, 'queued', ?, ?)
        """, (job_uuid, short_id, utc_now(), json.dumps(params, ensure_ascii=False)))
    return {"job_id": job_uuid, "short_id": short_id}