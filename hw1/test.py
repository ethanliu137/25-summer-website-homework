from pathlib import Path
import sqlite3, pandas as pd

DB_PATH = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\iedb_result.sqlite3"
hp_id = "O43493"  # 你要測的 UniProt

with sqlite3.connect(DB_PATH) as conn:
    # 列出表
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")]
    print("📦 這顆 DB 的表：", tables)

    # 各表是否存在
    print("iedb_result 在嗎？", "iedb_result" in tables)
    print("IEDB_human_correct 在嗎？", "IEDB_human_correct" in tables)

    # 該 hp_id 在兩表各有幾筆？
    cnt_enr = conn.execute(
        'SELECT COUNT(*) FROM "iedb_result" WHERE hit_human_protein_id=?', (hp_id,)
    ).fetchone()[0]
    cnt_proof = conn.execute(
        'SELECT COUNT(*) FROM "IEDB_human_correct" WHERE "UniProt_ID"=?', (hp_id,)
    ).fetchone()[0]
    print(f"iedb_result 中 {hp_id} 筆數：", cnt_enr)
    print(f"IEDB_human_correct 中 {hp_id} 筆數：", cnt_proof)

    # 看各取 3 筆 sample + 欄位
    if cnt_enr:
        print("\niedb_result 欄位：",
              [c[1] for c in conn.execute('PRAGMA table_info("iedb_result")')])
        print(pd.read_sql(
            'SELECT "MME(hit)","MME(hit)_start","MME(hit)_end","hit_human_protein_id" '
            'FROM "iedb_result" WHERE hit_human_protein_id=? LIMIT 3', conn, params=[hp_id]
        ))
    if cnt_proof:
        print("\nIEDB_human_correct 欄位：",
              [c[1] for c in conn.execute('PRAGMA table_info("IEDB_human_correct")')])
        print(pd.read_sql(
            'SELECT "IEDB IRI","Name","Starting Position","Ending Position","UniProt_ID" '
            'FROM "IEDB_human_correct" WHERE "UniProt_ID"=? LIMIT 3', conn, params=[hp_id]
        ))

    # 直接用一條 SQL 測 overlap（如果兩表都有資料，這裡應該 >0）
    if cnt_enr and cnt_proof:
        sql = """
        WITH mme AS (
          SELECT
            mme_hit,
            CAST(mme_hit__start AS INTEGER) AS mme_start,
            CAST(mme_hit__end   AS INTEGER) AS mme_end
            FROM iedb_result
            WHERE hit_human_protein_id = ?
        ),
        proofed AS (
          SELECT "IEDB IRI" AS IEDB_IRI,
                 "Name"     AS IEDB_epitope,
                 CAST("Starting Position" AS INTEGER) AS IEDB_start,
                 CAST("Ending Position"   AS INTEGER) AS IEDB_end
          FROM IEDB_human_correct
          WHERE "UniProt_ID" = ?
        )
        SELECT *
        FROM mme m
        JOIN proofed p
          ON NOT (m.mme_start > p.IEDB_end OR m.mme_end < p.IEDB_start)
        LIMIT 5;
        """
        df = pd.read_sql(sql, conn, params=[hp_id, hp_id])
        print("\n🔎 overlap 預覽：\n", df)
        print("overlap 筆數（前 5 項預覽）:", len(df))