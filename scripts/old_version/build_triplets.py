import os
import json
import time
import sqlite3
import pandas as pd
import re
from groq import Groq

MODEL = "llama-3.1-8b-instant"

client = Groq(api_key="")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "tables", "demographic_race.csv")
DB_PATH = os.path.join(BASE_DIR, "data", "my_database.db")
NL_FILE = os.path.join(BASE_DIR, "data", "nl_from_schema.jsonl")
OUT_PATH = os.path.join(BASE_DIR, "data", "eval_ready", "triplets_groq.csv_ready.jsonl")

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

SCHEMA_SQL = """
CREATE TABLE demographics (
    year INT,
    id TEXT,
    zipcode TEXT,
    race_total_population INT,
    one_race INT,
    two_or_more_races INT,
    white INT,
    black INT,
    american_indian_and_alaska_native INT,
    asian INT,
    native_hawaiian_and_other_pacific_islander INT,
    some_other_race INT,
    hispanic_or_latino_total INT,
    hispanic_or_latino INT,
    not_hispanic_or_latino INT
);
""".strip()


if not os.path.exists(DB_PATH):
    print("Creating SQLite DB from CSV...")
    df = pd.read_csv(CSV_PATH)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("demographics", conn, if_exists="replace", index=False)
    conn.close()
    print(f"DB created at {DB_PATH}")
else:
    print(f"DB already exists: {DB_PATH}")


SYSTEM_SQL = (
    "You are a precise SQL generator. Output a SINGLE valid SQLite SELECT query only. "
    "No explanations or commentary. Use only columns in the provided schema."
)
SYSTEM_JSON = (
    "Convert the SQL query into a valid JSON execution plan. "
    "Use this schema: "
    '{"operation": "string", "details": {"columns": [...], "table": "string"}, "children": []}. '
    "Return ONLY JSON."
)

def groq_chat(system: str, user: str, max_tokens=256, temperature=0.0) -> str:
    """Groq Chat wrapper"""
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return response.choices[0].message.content.strip()


def normalize_sql(sql: str) -> str:
    sql = re.sub(r"```(sql)?", "", sql, flags=re.I)
    sql = sql.replace("\n", " ").replace("\t", " ")
    sql = re.sub(r"\s+", " ", sql).strip()
    sql = re.sub(r"^.*?(SELECT\b.+)", r"\1", sql, flags=re.I | re.S)
    if not sql.endswith(";"):
        sql += ";"
    return sql.strip()

def validate_sql(conn, sql: str) -> bool:
    try:
        conn.execute(sql)
        return True
    except Exception:
        return False

def sql_from_nl(nl: str) -> str:
    prompt = f"Schema:\n{SCHEMA_SQL}\n\nNL: {nl}\nWrite only SQL."
    for _ in range(3):
        sql = groq_chat(SYSTEM_SQL, prompt)
        sql = normalize_sql(sql)
        if sql.lower().startswith("select"):
            return sql
        time.sleep(0.2)
    raise RuntimeError("Failed to generate SQL.")

def json_from_sql(sql: str) -> str:
    prompt = f"SQL: {sql}\nConvert to JSON execution plan."
    for _ in range(3):
        out = groq_chat(SYSTEM_JSON, prompt, max_tokens=384)
        out = out.strip().strip("`")
        m = re.search(r"(\{.*\})", out, flags=re.S)
        if m:
            return m.group(1)
        time.sleep(0.2)
    raise RuntimeError("Failed to generate JSON plan.")


def main():
    with open(NL_FILE, "r", encoding="utf-8") as f:
        nls = [json.loads(line).get("nl", "").strip() for line in f if line.strip()]
    print(f" Loaded {len(nls)} NL queries from {NL_FILE}")

    conn = sqlite3.connect(DB_PATH)
    kept, skipped = 0, 0

    with open(OUT_PATH, "w", encoding="utf-8") as out:
        for i, nl in enumerate(nls, 1):
            try:
                sql = sql_from_nl(nl)
                if not validate_sql(conn, sql):
                    skipped += 1
                    continue
                json_plan = json_from_sql(sql)
                rec = {
                    "nl": nl,
                    "sql": sql,
                    "json_plan": json.loads(json_plan),
                    "validated": True,
                    "provenance": {"generator": MODEL, "schema": "demographics"}
                }
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                kept += 1

            except Exception as e:
                print(f"[ERROR] Failed to process NL: {nl}")
                print(f"   L_ Reason: {str(e)}")
                skipped += 1
                continue

            if i % 25 == 0:
                print(f"[{i}/{len(nls)}] kept={kept} skipped={skipped}")

    conn.close()
    print(f"\n Done! {kept} triplets saved → {OUT_PATH}")

if __name__ == "__main__":
    main()