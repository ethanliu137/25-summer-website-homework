# web_tool/utils/View_by_Epitope.py
from __future__ import annotations
import sqlite3
from pathlib import Path
import pandas as pd

# === 你的 SQLite 設定（照你提供）===
DB_PATH        = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\iedb_result.sqlite3"
TABLE_ENR      = "iedb_result"      # 來源：IEDB enriched 後的表
VIEW_EPI_TABLE = "view_by_epitope"  # 目的地：要給頁面讀的表

def build_view_by_epitope(
    db_path: str = DB_PATH,
    src_table: str = TABLE_ENR,
    dst_table: str = VIEW_EPI_TABLE,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    從 IEDB enriched 表 (src_table) 讀資料，做必要整理，寫成 view_by_epitope（覆蓋）。
    目前預設做「直拷貝＋欄位排序＋去重」；你要做彙總或重整，只要改這裡。
    """
    sql = f'SELECT * FROM "{src_table}"'
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(sql, conn)

        # 你可以在這裡做「轉欄位」「彙總」「刪除多餘欄位」等操作
        # 例：只保留常用欄位（如果欄位存在就保留）
        prefer_cols = [
            # MME/IEDB 常見欄位（注意：DB 內通常已經是 snake_case）
            "query_protein_name",
            "mme_query", "mme_query_start", "mme_query_end",
            "hit_human_protein_id", "hit_human_protein_name",
            "mme_hit_start", "mme_hit_end",
            "iedb_human_epitope_substring_count",
            "iedb_human_protein_data_count",
            "iedb_human_positional_fully_contained",
            "iedb_human_positional_partial_overlap",
        ]
        keep = [c for c in prefer_cols if c in df.columns]
        if keep:
            df = df[keep + [c for c in df.columns if c not in keep]]

        # 去重（保守做法）
        df = df.drop_duplicates().reset_index(drop=True)

        # 覆蓋寫回 view 表
        df.to_sql(dst_table, conn, if_exists="replace", index=False)

        # 回傳給呼叫端（可選）
        return df

if __name__ == "__main__":
    out = build_view_by_epitope()
    print(f"✅ view_by_epitope 已更新，筆數：{len(out)}")