import sqlite3
import pandas as pd

DB_PATH = r"C:\Users\ethan\Desktop\碩班\暑假\web_hw\web_hw\hw1\hw1\iedb_result.sqlite3"
CSV_PATH = r"C:\Users\ethan\Desktop\碩班\暑假\hw6\human_protein_detail.csv"

df = pd.read_csv(CSV_PATH)

with sqlite3.connect(DB_PATH) as conn:
    df.to_sql("human_protein_detail", conn, if_exists="replace", index=False)

print("done")