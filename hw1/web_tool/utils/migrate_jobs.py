# web_tool/utils/migrate_jobs.py
import sqlite3

DB_PATH = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\iedb_result.sqlite3"

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS jobs (
    job_id      TEXT PRIMARY KEY,
    status      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    started_at  TEXT,
    finished_at TEXT,
    params_json TEXT,
    message     TEXT
);

-- 新增：「job 產生的 IEDB 資料表」登記簿
CREATE TABLE IF NOT EXISTS job_artifacts (
    job_id     TEXT PRIMARY KEY,
    iedb_table TEXT NOT NULL,
    row_count  INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_artifacts_table ON job_artifacts(iedb_table);
"""

def run():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for stmt in filter(None, DDL.split(";")):
            s = stmt.strip()
            if not s: 
                continue
            try:
                cur.execute(s + ";")
            except sqlite3.OperationalError as e:
                # 忽略「重複加欄位」之類的錯（代表你已經跑過了）
                if "duplicate column name" in str(e).lower():
                    pass
                else:
                    raise
        conn.commit()
    print("Migration done.")

if __name__ == "__main__":
    run()