import os
import time
import re
from groq import Groq

MODEL_NAME = "llama-3.1-8b-instant"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_SQL_FILE = os.path.join(BASE_DIR, "data", "raw_sql", "random_sql_queries.txt")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "nl_plans", "nl_generated.txt")

client = Groq(api_key="")

SCHEMA_FOR_PROMPT = """
You are a data analyst converting SQL to natural language.

Database schema:
Table: demographics
Description: Demographic and race data by US zipcode and year.
Columns:
- year: Calendar year of data
- id: Unique identifier
- zipcode: The ZIP code tabulation area (ZCTA5)
- race_total_population: Total population for racial data
- one_race: Population of one race
- two_or_more_races: Population of two or more races
- white: White population
- black: Black or African American population
- american_indian_and_alaska_native: American Indian and Alaska Native population
- asian: Asian population
- native_hawaiian_and_other_pacific_islander: Native Hawaiian and Other Pacific Islander population
- some_other_race: Population of some other race
- hispanic_or_latino_total: Total Hispanic or Latino population
- hispanic_or_latino: Hispanic or Latino population
- not_hispanic_or_latino: Not Hispanic or Latino population

When asked to translate SQL, use plain English that clearly reflects these columns.
Avoid SQL keywords. Be concise and natural.
"""


def sql_to_nl(sql_query: str) -> str:
    """Convert SQL query into a short, human-like NL question."""
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SCHEMA_FOR_PROMPT},
                {"role": "user", "content": f"Convert the following SQL into a natural language question:\n{sql_query}"}
            ],
            temperature=0.6,
        )

        nl = response.choices[0].message.content.strip()

        nl = nl.strip('"').strip("'")
        nl = re.sub(r"^(The query|This query|It|This SQL)[^?]*", "", nl, flags=re.I).strip()

        return nl

    except Exception as e:
        print(f"[!] API Error: {e}. Retrying after 1 second...")
        time.sleep(1)
        return sql_to_nl(sql_query)


def main():
    if not os.path.exists(RAW_SQL_FILE):
        print(f"File not found: {RAW_SQL_FILE}")
        return

    with open(RAW_SQL_FILE, "r") as f:
        sql_queries = [line.strip() for line in f.readlines() if line.strip()]

    print(f"Generating NL questions for {len(sql_queries)} SQL queries (optimized for token usage)...")

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        for i, sql_query in enumerate(sql_queries, start=1):
            nl = sql_to_nl(sql_query)
            out_f.write(f"{sql_query}\n→ {nl}\n\n")
            print(f"[{i:03d}] {nl[:80]}")
            time.sleep(0.3)

    print(f"\nAll {len(sql_queries)} NL questions generated!")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()