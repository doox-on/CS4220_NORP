import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from groq import Groq
from tqdm import tqdm


GROQ_API_KEY = "gsk_..." 
MODEL_NAME = "llama-3.1-8b-instant"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "generated_sqls.jsonl")

TOTAL_SQL_COUNT = 100   
BATCH_SIZE = 20 
MAX_WORKERS = 10

DB_SCHEMA = """
Table: demographics
Columns:
- year (INTEGER)
- id (TEXT)
- zipcode (TEXT)
- race_total_population (INTEGER)
- one_race (INTEGER)
- two_or_more_races (INTEGER)
- white (INTEGER)
- black (INTEGER)
- american_indian_and_alaska_native (INTEGER)
- asian (INTEGER)
- native_hawaiian_and_other_pacific_islander (INTEGER)
- some_other_race (INTEGER)
- hispanic_or_latino_total (INTEGER)
- hispanic_or_latino (INTEGER)
- not_hispanic_or_latino (INTEGER)
"""

client = Groq(api_key=GROQ_API_KEY)

def generate_sqls_batch(count):
    prompt = f"""
    You are a SQL generator for a SQLite database.
    
    Schema:
    {DB_SCHEMA}

    Task: Generate exactly {count} diverse SQL queries.
    
    Guidelines:
    1. Output ONLY raw SQL queries.
    2. One query per line.
    3. Do NOT use markdown or numbering (e.g., "1. SELECT...").
    4. Use various complexities (Simple SELECT, WHERE, SUM/AVG, GROUP BY, ORDER BY).
    5. Use valid column names provided in the schema.
    """

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8, # 다양성을 위해 약간 높게 설정
        )
        content = response.choices[0].message.content.strip()
        
        sqls = [line.strip() for line in content.split('\n') if line.strip().upper().startswith("SELECT")]
        return sqls
        
    except Exception as e:
        print(f"Batch Gen Error: {e}")
        return []

def main():
    num_batches = (TOTAL_SQL_COUNT // BATCH_SIZE) + 1
    
    print(f"Generating {TOTAL_SQL_COUNT} SQL queries...")
    print(f"   - Batch Size: {BATCH_SIZE}")
    print(f"   - Parallel Workers: {MAX_WORKERS}")
    print(f"   - Target File: {OUTPUT_FILE}\n")

    saved_count = 0
    generated_sqls_set = set()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(generate_sqls_batch, BATCH_SIZE) for _ in range(num_batches)]
            
            for future in tqdm(as_completed(futures), total=num_batches, desc="Processing"):
                batch_sqls = future.result()
                
                for sql in batch_sqls:
                    if saved_count >= TOTAL_SQL_COUNT:
                        break
                    
                    if sql not in generated_sqls_set:
                        entry = {
                            "id": saved_count + 1,
                            "sql": sql,
                            "nl": "" 
                        }
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        generated_sqls_set.add(sql)
                        saved_count += 1
                        
                f.flush()

    print(f"\nDone! Saved {saved_count} unique SQL queries to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()