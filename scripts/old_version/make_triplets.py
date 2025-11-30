import os
import json
import glob

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

RAW_SQL_FILE = os.path.join(DATA_DIR, "raw_sql", "random_sql_queries.txt")
NL_FILE = os.path.join(DATA_DIR, "nl_plans", "nl_generated.txt")
JSON_DIR = os.path.join(DATA_DIR, "json_plans")
OUTPUT_FILE = os.path.join(DATA_DIR, "merged_dataset.jsonl")

def load_sql_nl_pairs(path):
    pairs = []
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
        current_sql, current_nl = None, None
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith("SELECT"):
                current_sql = line
            elif line.startswith("→"):
                current_nl = line.replace("→", "").strip().strip('"')
                if current_sql and current_nl:
                    pairs.append((current_sql, current_nl))
                    current_sql, current_nl = None, None
    return pairs

def load_valid_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not data or (isinstance(data, dict) and len(data) == 0):
            return None

        if "error" in data or "raw_output" in data:
            return None

        if "operation" not in data:
            return None
        return data
    except Exception:
        return None

def main():
    sql_nl_pairs = load_sql_nl_pairs(NL_FILE)
    json_files = sorted(glob.glob(os.path.join(JSON_DIR, "*.json")))

    print(f"SQL-NL pairs: {len(sql_nl_pairs)} | JSON files: {len(json_files)}")

    merged = []
    skipped = 0

    for i, ((sql, nl), jpath) in enumerate(zip(sql_nl_pairs, json_files), start=1):
        json_data = load_valid_json(jpath)
        if not json_data:
            skipped += 1
            continue

        merged.append({
            "id": i,
            "nl": nl,
            "sql": sql,
            "json_plan": json_data
        })

    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for item in merged:
            out.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"\n Merge Success! Valid Triplets: {len(merged)}")
    print(f"Skipped JSON files: {skipped} (Error, Empty)")
    print(f"File saved in : {OUTPUT_FILE}")

if __name__ == "__main__":
    main()