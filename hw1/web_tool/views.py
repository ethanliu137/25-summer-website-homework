# web_tool/views.py
# -*- coding: utf-8 -*-
import io, csv, re, sqlite3
from pathlib import Path
import pandas as pd

from django.http import JsonResponse, HttpResponseBadRequest, HttpResponse
from django.shortcuts import render
from django.views.decorators.http import require_POST, require_GET

# MME 工具
from .utils.mme_pipline import run_pipeline, save_append

# IEDB 核心函式
from .utils.IEDB_pipline import process as iedb_process

# 分頁工具
from .utils.View_by_Epitope import build_view_by_epitope
from web_tool.utils.view_by_query import build_summary_by_query, DB_PATH

# 產生JOB_ID
from .utils.jobs import create_job

# ---------------------------------------------------------
# 常數設定
# ---------------------------------------------------------
# 參考資料 FASTA（請確認檔案存在）
HUMAN_FASTA = Path(__file__).resolve().parent / "static" / "web_tool" / "ref" / "human.fasta"

# IEDB 參考 CSV（路徑建議用相對本 app 的方式，不怕換機）
IEDB_CSV = str(
    Path(__file__).resolve().parent / "static" / "web_tool" / "ref" / "IEDB_human_correct.csv"
)

# SQLite 檔（統一都寫到這個 DB）
DB_PATH   = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\iedb_result.sqlite3"
TABLE_RAW = "mme_result"     # 原始 MME
TABLE_ENR = "iedb_result"    # IEDB enriched
VIEW_EPI_TABLE = "view_by_epitope"
# Detail頁面
TABLE_IEDB_PROOFED = "IEDB_human_correct"

# ---------------------------------------------------------
# 首頁
# ---------------------------------------------------------
def mme_form_page(request):
    return render(request, "hw.html")

# ---------------------------------------------------------
# 共用小工具
# ---------------------------------------------------------
def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [re.sub(r'[^0-9a-zA-Z_]+', '_', c).strip('_').lower() for c in out.columns]
    return out

def _save_iedb_enriched(df_enr: pd.DataFrame) -> int:
    """寫入 TABLE_ENR（snake_case 欄位），回傳本次寫入筆數"""
    sdf = _sanitize_columns(df_enr)
    n_added = len(sdf)
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        sdf.to_sql(TABLE_ENR, conn, if_exists="append", index=False)
    return n_added

# ---------------------------------------------------------
# 後端 API：表單提交 → 跑 MME & IEDB → 存 DB → 從 DB 讀回當批 → 回 JSON/CSV
# ---------------------------------------------------------
@require_POST
def mme_form(request):
    is_ajax = request.headers.get("x-requested-with") == "XMLHttpRequest"

    # 1) 驗證 k
    k_str = (request.POST.get("k_mer") or "").strip()
    if not k_str:
        return HttpResponseBadRequest("請輸入 k-mer 長度")
    try:
        k = int(k_str)
    except ValueError:
        return HttpResponseBadRequest("k-mer 必須是整數")
    if not (1 <= k <= 1000):
        return HttpResponseBadRequest("k-mer 必須介於 1 到 1000 之間")

    # 2) species 與 human fasta
    species = request.POST.get("species", "human")
    if species != "human":
        return HttpResponseBadRequest("目前僅支援 human 參考資料")
    human_path = str(HUMAN_FASTA)
    if not Path(human_path).exists():
        return HttpResponseBadRequest(f"參考 FASTA 不存在：{human_path}")

    # 3) 取得 query（檔案優先，否則 textarea）
    up = request.FILES.get("query_fasta")
    txt = (request.POST.get("query_fasta") or "").strip()
    if not up and not txt:
        return HttpResponseBadRequest("請貼上 FASTA 或上傳檔案")

    if up:
        raw = up.read()
        try:
            q_text = raw.decode("utf-8")
        except UnicodeDecodeError:
            q_text = raw.decode("utf-8", errors="ignore")
    else:
        q_text = txt
    q_file_like = io.StringIO(q_text)

    # 4) 跑 MME
    try:
        df_raw = run_pipeline(q_file_like, human_path, k=k)
    except Exception as e:
        return HttpResponseBadRequest(f"運行失敗：{e}")

    # 5) （可選）存原始 MME
    try:
        save_append(df_raw, db_path=DB_PATH, table=TABLE_RAW)
    except Exception as e:
        # 寫庫失敗不影響回應 IEDB 結果
        print(f"⚠️ 寫入 {TABLE_RAW} 失敗：{e}", flush=True)

    # 6) 跑 IEDB enrich
    try:
        iedb_df = pd.read_csv(IEDB_CSV, encoding="utf-8-sig")
    except Exception as e:
        return HttpResponseBadRequest(f"讀取 IEDB CSV 失敗：{e}")
    try:
        df_enr = iedb_process(df_raw.copy(), iedb_df)
    except Exception as e:
        return HttpResponseBadRequest(f"IEDB 運行失敗：{e}")

    # 7) 存 IEDB enriched（不加 batch_id）
    n_added = _save_iedb_enriched(df_enr)

    # 7.1) ★ 自動重建 view_by_epitope（若工具支援）
    try:
        build_view_by_epitope(DB_PATH, src_table=TABLE_ENR, dst_table=VIEW_EPI_TABLE)
    except Exception as e:
        print(f"⚠️ 重建 {VIEW_EPI_TABLE} 失敗：{e}", flush=True)
        
    # 8) 從 DB 讀回「本批」資料：用 rowid 倒序取最新 n_added 筆，再反轉回原順序
    with sqlite3.connect(DB_PATH) as conn:
        df_show = pd.read_sql(
            f'SELECT * FROM "{TABLE_ENR}" ORDER BY rowid DESC LIMIT ?',
            conn, params=[n_added]
        )
    
    # 🔹把 batch_id 從回傳結果移除（舊資料立刻不顯示）
    df_show = df_show.drop(columns=["batch_id"], errors="ignore")
    
    # 反轉回寫入順序（可選）
    if not df_show.empty:
        df_show = df_show.iloc[::-1].reset_index(drop=True)

    # 9) 回傳
    if is_ajax:
        return JsonResponse({
            "source": "iedb_enriched",
            "db_path": DB_PATH,
            "table": TABLE_ENR,
            "columns": list(df_show.columns),
            "records": df_show.to_dict(orient="records"),
        }, safe=False)

    csv_text = df_show.to_csv(index=False)
    resp = HttpResponse(csv_text, content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = f'attachment; filename="iedb_enriched_k{k}.csv"'
    return resp

# ---------------------------------------------------------
# JOB_ID
# ---------------------------------------------------------
@require_GET
def api_create_job(request):
    """
    輕量：不需參數。第一次進主頁時叫一次，拿到一組 job。
    之後存在 localStorage 就不再叫。
    """
    job = create_job(params={})  # 你要放什麼預設參數都可以
    return JsonResponse(job)

def job_id_search(request):
    return render(request, "job_id_search.html")
# ---------------------------------------------------------
# 顯示頁（DataTables 容器）
# ---------------------------------------------------------
def View_Perfect_Match_Table(request):
    return render(request, "Perfect_Table.html")

def View_by_Eptiope(request):
    return render(request, "view_by_eptiope.html")

def View_by_Query(request):
    return render(request, "view_by_query.html")

def View_by_Reference(request):
    return render(request, "view_by_reference.html")

def reference_detail(request, id):
    return render(request, "web_tool/view_by_ref_detail.html", {"id": id})
# ---------------------------------------------------------
# 提供 IEDB 結果的 JSON（如果你有獨立頁面需要直接讀 DB 顯示）
# ---------------------------------------------------------
@require_GET
def iedb_from_sqlite(request):
    """讀 DB 的 IEDB enriched（TABLE_ENR）→ 回 {columns, records}"""
    limit = request.GET.get("limit")
    limit = int(limit) if (limit and str(limit).isdigit()) else None

    sql = f'SELECT * FROM "{TABLE_ENR}"'
    if limit is not None:
        sql += f" LIMIT {limit}"

    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql(sql, conn)
    except Exception as e:
        return HttpResponseBadRequest(f"讀取 SQLite 失敗：{e}")

    return JsonResponse({
        "columns": list(df.columns),
        "records": df.to_dict(orient="records")
    }, safe=False)

@require_GET
def View_by_Epitope_data(request):
    # 可選：依 query_protein_name / epitope 篩
    q   = (request.GET.get("q") or "").strip()
    epi = (request.GET.get("epitope") or "").strip()
    lim = (request.GET.get("limit") or "").strip()
    limit = int(lim) if lim.isdigit() else None

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # 這裡改查「真實表」TABLE_ENR（= iedb_result），不要用 VIEW_EPI_TABLE
            # 因為我們要的欄位與正確的聚合口徑都在真實表裡。
            cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{TABLE_ENR}")')]
            needed = {"mme_query", "query_protein_name", "hit_human_protein_id", "iedb_human_epitope_substring_count"}
            missing = sorted(list(needed - set(cols)))
            if missing:
                return HttpResponseBadRequest(f"表 '{TABLE_ENR}' 缺少欄位：{missing}。目前欄位：{cols}")

            # WHERE（僅作為前置篩選，不影響粒度）
            where_parts, params = [], []
            if q:
                where_parts.append("query_protein_name = ?")
                params.append(q)
            if epi:
                where_parts.append("mme_query = ?")
                params.append(epi)
            where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

            # 以「epitope（mme_query）」為唯一粒度的聚合
            # epitope_count                   = 該 epitope 的列數（或想更嚴格可改 DISTINCT hit+pos）
            # hit_human_protein_id_kind      = 該 epitope 命中的不同蛋白數
            # query_protein_name_kind        = 該 epitope 來自幾個不同的 query 名稱
            # epitope_length                 = epitope 長度
            # IEDB_human_epitope_substring_count = 對應 IEDB 的 substring 計數（此欄對同一 epitope 通常相同，取 MAX/MIN 都可）
            
            sql = f"""
            WITH base AS (
            SELECT DISTINCT
                TRIM(COALESCE(mme_query,''))                 AS Epitope,
                TRIM(COALESCE(hit_human_protein_id,''))      AS hit_id,
                TRIM(COALESCE(query_protein_name,''))        AS qname,
                COALESCE(iedb_human_epitope_substring_count,0) AS substr_cnt
            FROM "{TABLE_ENR}"
            {where_sql}
            )
            SELECT
            Epitope,
            -- 這裡的 COUNT(*) 就是以 base 的「去重結果」計數，不會被 *15 放大
            COUNT(*)                                       AS epitope_count,
            COUNT(DISTINCT hit_id)                         AS hit_human_protein_id_kind,
            COUNT(DISTINCT qname)                          AS query_protein_name_kind,
            LENGTH(Epitope)                                AS epitope_length,
            MAX(substr_cnt)                                AS IEDB_human_epitope_substring_count
            FROM base
            GROUP BY Epitope
            ORDER BY epitope_count DESC, Epitope ASC
            { "LIMIT ?" if limit is not None else "" }
            """
            if limit is not None:
                params.append(limit)

            print("[View_by_Epitope_data] SQL:\n", sql)
            print("[View_by_Epitope_data] params:", params)

            df = pd.read_sql(sql, conn, params=params)

    except Exception as e:
        return HttpResponseBadRequest(f"讀取/聚合 SQLite 失敗：{e}")

    return JsonResponse({"columns": list(df.columns), "data": df.values.tolist()})

@require_GET
def View_by_Query_data(request):
    """
    GET 參數：
      - ?q= 某個 query_protein_name（可選）
      - ?limit= 2000（可選，對聚合後結果加 LIMIT）
    回傳 {columns, data} 給 DataTables。
    """
    q = (request.GET.get("q") or "").strip()
    lim = request.GET.get("limit")
    limit = int(lim) if (lim and lim.isdigit()) else None

    # 診斷（可留著幫你確認是不是同一顆 DB）
    print("[View_by_Query_data] DB exists?", Path(DB_PATH).exists(), DB_PATH)
    try:
        df = build_summary_by_query(filter_query=q if q else None, limit=limit)
    except Exception as e:
        return HttpResponseBadRequest(f"讀取/聚合 SQLite 失敗：{e}")

    return JsonResponse({
        "columns": list(df.columns),
        "data": df.values.tolist()
    })

@require_GET
def View_by_Reference_data(request):
    """
    以 hit_human_protein_id 彙總：
      - query_protein_name_kind          = COUNT(DISTINCT query_protein_name)
      - epitope_count                    = COUNT(DISTINCT TRIM(mme_query))
      - IEDB_fully_contained_MME_count   = 有 fully!=0 的「唯一 epitope」數
      - IEDB_fully_or_partial_MME_count  = 有 fully!=0 或 partial!=0 的「唯一 epitope」數
    """
    hit_id = (request.GET.get("id") or "").strip()
    limit  = request.GET.get("limit")
    limit  = int(limit) if (limit and str(limit).isdigit()) else None

    params = []
    where  = ""
    if hit_id:
        where = "WHERE hit_human_protein_id = ?"
        params.append(hit_id)

    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT ?"
        params.append(limit)

    sql = f"""
        SELECT
          hit_human_protein_id,
          COUNT(DISTINCT query_protein_name) AS query_protein_name_kind,
          COUNT(DISTINCT TRIM(mme_query))    AS epitope_count,
          MIN(iedb_human_protein_data_count) AS IEDB_human_protein_data_count,
          COUNT(DISTINCT CASE
                  WHEN COALESCE(iedb_human_positional_fully_contained,0) != 0
                  THEN TRIM(mme_query) END)  AS IEDB_fully_contained_MME_count,
          COUNT(DISTINCT CASE
                  WHEN COALESCE(iedb_human_positional_fully_contained,0) != 0
                       OR COALESCE(iedb_human_positional_partial_overlap,0) != 0
                  THEN TRIM(mme_query) END)  AS IEDB_fully_or_partial_MME_count
        FROM "{TABLE_ENR}"
        {where}
        GROUP BY hit_human_protein_id
        ORDER BY hit_human_protein_id ASC
        {limit_clause}
    """

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # 也可先檢查欄位是否存在，避免打錯欄位名：
            # cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{TABLE_ENR}")')]
            # print("columns:", cols)
            df = pd.read_sql(sql, conn, params=params)
    except Exception as e:
        return HttpResponseBadRequest(f"讀取/聚合 SQLite 失敗：{e}")

    return JsonResponse({"columns": list(df.columns), "data": df.values.tolist()})

# ---------------------------------------------------------
# Reference 的 detail頁
# ---------------------------------------------------------
@require_GET
def view_by_ref_detail(request):
    hp_id = (request.GET.get("id") or "").strip()   # hit_human_protein_id
    if not hp_id:
        return HttpResponseBadRequest("缺少 id")

    with sqlite3.connect(DB_PATH) as conn:
        # 表1：最上面的 Human Protein 基本資訊
        sql_basic = """
            SELECT 
                Uniprot_protein,
                Gene_description,
                Gene_HGNC,
                Ensembl
            FROM human_protein_detail
            WHERE Uniprot_protein = ?
            LIMIT 1
        """
        basic_df = pd.read_sql(sql_basic, conn, params=[hp_id])
        basic_info = basic_df.to_dict(orient="records")[0] if not basic_df.empty else {
            "Uniprot_protein": hp_id,
            "Gene_description": "N/A",
            "Gene_HGNC": "N/A",
            "Ensembl": "N/A"
        }

        # 表2：IEDB proofed Epitope in Human Protein
        sql_proofed = f'''
            SELECT 
                "IEDB IRI",
                "Name",
                CAST("Starting Position" AS INTEGER) AS "Starting Position",
                CAST("Ending Position"   AS INTEGER) AS "Ending Position",
                "Molecule Parent",
                "Molecule Parent IRI",
                "Source Organism",
                "UniProt_ID"
            FROM "{TABLE_IEDB_PROOFED}"
            WHERE "UniProt_ID" = ?
        '''
        proofed_df = pd.read_sql(sql_proofed, conn, params=[hp_id])

        # 表3：IEDB overlapped perfect match Epitope
        sql_mme = f'''
            SELECT
                mme_hit,
                CAST(mme_hit__start AS INTEGER) AS mme_start,
                CAST(mme_hit__end   AS INTEGER) AS mme_end
            FROM "{TABLE_ENR}"
            WHERE hit_human_protein_id = ?
        '''
        mme_df = pd.read_sql(sql_mme, conn, params=[hp_id])
        mme_df["mme_start"] = pd.to_numeric(mme_df["mme_start"], errors="coerce")
        mme_df["mme_end"]   = pd.to_numeric(mme_df["mme_end"],   errors="coerce")

        # 取 IEDB_human_correct（表2）必要欄位
        proofed_min = proofed_df.rename(columns={
            "IEDB IRI": "IEDB_IRI",
            "Name": "IEDB_epitope",
            "Starting Position": "IEDB_start",
            "Ending Position": "IEDB_end",
        })[["IEDB_IRI", "IEDB_epitope", "IEDB_start", "IEDB_end", "UniProt_ID"]].copy()

        for col in ["IEDB_start", "IEDB_end"]:
            proofed_min[col] = pd.to_numeric(proofed_min[col], errors="coerce").astype("Int64")

        overlap_records = []
        if not mme_df.empty and not proofed_min.empty:
            for _, m in mme_df.iterrows():
                start = m["mme_start"]; end = m["mme_end"]
                if pd.isna(start) or pd.isna(end):
                    continue
                for _, p in proofed_min.iterrows():
                    s_pos = p["IEDB_start"]; e_pos = p["IEDB_end"]
                    if pd.isna(s_pos) or pd.isna(e_pos):
                        continue
                    # 是否重疊
                    if not ((start <= e_pos) and (end >= s_pos)):
                        continue
                    # 完全包含 or 部分重疊
                    perfect = (start >= s_pos) and (end <= e_pos)
                    pos_rel = "IEDB includes Perfect_Match" if perfect else "partial overlap"
                    overlap_records.append({
                        "IEDB_IRI":              p["IEDB_IRI"],
                        "IEDB_human_protein_id": hp_id,
                        "IEDB_epitope":          p["IEDB_epitope"],
                        "IEDB_start":            int(s_pos),
                        "IEDB_end":              int(e_pos),
                        "mme_hit":               m["mme_hit"],
                        "mme_hit__start":        int(start),
                        "mme_hit__end":          int(end),
                        "position_relationship": pos_rel,
                    })

        overlap_df = pd.DataFrame(overlap_records)
        if not overlap_df.empty:
            overlap_df = overlap_df.drop_duplicates()

        overlap_columns = [
            "IEDB_IRI",
            "IEDB_human_protein_id",
            "IEDB_epitope",
            "IEDB_start",
            "IEDB_end",
            "mme_hit",
            "mme_hit__start",
            "mme_hit__end",
            "position_relationship",
        ]
        overlap_rows = overlap_df.to_dict(orient="records") if not overlap_df.empty else []

        # 表4：Result Table（補回，並在最後取 rows）
        sql_result = f"""
            SELECT
                query_protein_name,
                COUNT(DISTINCT TRIM(mme_query)) AS epitope_count
            FROM "{TABLE_ENR}"
            WHERE hit_human_protein_id = ?
            GROUP BY query_protein_name
            ORDER BY query_protein_name
        """
        result_df = pd.read_sql(sql_result, conn, params=[hp_id]).fillna("")
        result_rows = result_df.to_dict(orient="records")

        # 表5：Epitope Plot Detail（白名單 + 固定順序）
        # ===== 表5：Epitope Plot Detail（動態欄位，排除不需要的） =====
        # 直接從 iedb_result 撈同一個蛋白 ID 的 1 筆資料
        sql_enr = f'''
            SELECT *
            FROM "{TABLE_ENR}"
            WHERE hit_human_protein_id = ?
            ORDER BY ROWID DESC   -- 想取最舊就改 ASC；不在意可拿掉 ORDER BY
            LIMIT 1
        '''
        enr_df = pd.read_sql(sql_enr, conn, params=[hp_id])

        # 丟掉不想要的欄位（有就丟，沒有就跳過）
        cols_to_drop = [c for c in ("job", "job_id", "batch", "batch_id") if c in enr_df.columns]
        if cols_to_drop:
            enr_df = enr_df.drop(columns=cols_to_drop)

        # 轉空值，避免前端顯示 nan
        enr_df = enr_df.fillna("")

        # **最後一刻**才產生 columns / rows（避免表頭/表身不一致）
        enr_columns = list(enr_df.columns)
        enr_rows    = enr_df.to_dict(orient="records")

    return render(request, "view_by_ref_detail.html", {
        "hp_id": hp_id,

        # 表1
        "basic_info": basic_info,

        # 表2（Proofed）
        "columns": list(proofed_df.columns),
        "rows": proofed_df.fillna("").to_dict(orient="records"),

        # 表3（Overlap）
        "overlap_columns": overlap_columns,
        "overlap_rows": overlap_rows,

        # 表4（Result）
        "result_rows": result_rows,

        # 表5（ENR 原始）
        "enr_columns": enr_columns,
        "enr_rows": enr_rows,
    })