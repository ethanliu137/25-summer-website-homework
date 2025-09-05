
import uuid, sqlite3, datetime
from web_tool.utils.view_by_query import DB_PATH
from web_tool.views import _assert_safe_table

def utc_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

job_id = str(uuid.uuid4())
table  = "iedb_" + job_id.replace("-", "")[:10]
_assert_safe_table(table)

with sqlite3.connect(DB_PATH) as conn:
    # 從舊表複製幾筆測試資料到新表
    conn.execute(f'''
        CREATE TABLE "{table}" AS
        SELECT mme_query, query_protein_name, hit_human_protein_id, iedb_human_epitope_substring_count        
        FROM iedb_result
        LIMIT 50
    ''')

    # 加索引
    conn.execute(f'CREATE INDEX IF NOT EXISTS "{table}_idx_q" ON "{table}"(query_protein_name)')
    conn.execute(f'CREATE INDEX IF NOT EXISTS "{table}_idx_e" ON "{table}"(mme_query)')

    # 註冊到 job_artifacts
    row_count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    conn.execute('INSERT OR REPLACE INTO job_artifacts (job_id, iedb_table, row_count, created_at) VALUES (?,?,?,?)',
                 (job_id, table, row_count, utc_now()))
    conn.commit()

print("SMOKE TEST job_id =", job_id, "table =", table, "rows =", row_count)
