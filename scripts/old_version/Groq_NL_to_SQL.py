import os
import time
import json
import re
from tqdm import tqdm
from groq import Groq

MODEL_NAME = "llama-3.1-8b-instant"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TRIPLET_FILE = os.path.join(BASE_DIR, "data", "triplets.jsonl")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "eval_ready", "groq_nl2sql_results.jsonl")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

client = Groq(api_key=os.getenv("GROQ_API_KEY", ""))


SYSTEM_PROMPT = """
You are a senior data engineer who writes SQL queries precisely and concisely.
Your job is to translate the user's natural language question into a valid SQL query.
Follow the schema and avoid unnecessary explanation text.

Database schema:
Table: demographics
Columns:
- year
- id
- zipcode
- race_total_population
- one_race
- two_or_more_races
- white
- black
- american_indian_and_alaska_native
- asian
- native_hawaiian_and_other_pacific_islander
- some_other_race
- hispanic_or_latino_total
- hispanic_or_latino
- not_hispanic_or_latino
"""


def nl_to_sql(nl_query: str) -> str:
    prompt = f"Convert the following question into an SQL query:\n\n{nl_query}\n\nReturn only the SQL query."

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT.strip()},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=300
        )

        sql = response.choices[0].message.content.strip()

        if sql.startswith("```"):
            sql = re.sub(r"^```(sql)?", "", sql)
            sql = sql.replace("```", "").strip()

        if not sql.endswith(";"):
            sql += ";"

        return sql
    except Exception as e:
        print(f"[!] Error: {e}. Retrying...")
        time.sleep(1)
        return nl_to_sql(nl_query)


triplets = []
with open(TRIPLET_FILE, "r", encoding="utf-8") as f:
    for line in f:
        try:
            triplets.append(json.loads(line))
        except json.JSONDecodeError:
            continue

print(f"Loaded {len(triplets)} triplets for NL→SQL conversion")


results = []
for i, item in enumerate(tqdm(triplets, desc="Converting NL→SQL via Groq"), start=1):
    nl = item.get("nl", "").strip()
    gold_sql = item.get("sql", "").strip()

    pred_sql = nl_to_sql(nl)

    results.append({
        "nl": nl,
        "pred_sql": pred_sql,
        "gold_sql": gold_sql
    })

    if i % 10 == 0:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

print(f"\nDone! {len(results)} NL→SQL conversions saved.")
print(f"File path: {OUTPUT_FILE}")
print("You can now evaluate this file using query_comparator or query_tester.")