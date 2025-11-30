import os
import json

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# New JSONL-based inputs
NL_SQL_FILE = os.path.join(DATA_DIR, "demographics_queries.jsonl")
JSON_PLAN_FILE = os.path.join(DATA_DIR, "converted_plans.jsonl")

OUTPUT_FILE = os.path.join(DATA_DIR, "merged_dataset.jsonl")

def load_jsonl(path):
    items = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            items[obj["id"]] = obj
    return items

def main():
    print("Loading NL/SQL jsonl...")
    nl_sql = load_jsonl(NL_SQL_FILE)

    print("Loading JSON plans jsonl...")
    plans = load_jsonl(JSON_PLAN_FILE)

    merged = []
    missing = 0

    for id_, obj in nl_sql.items():
        if id_ not in plans:
            missing += 1
            continue

        merged.append({
            "id": id_,
            "nl": obj.get("nl"),
            "sql": obj.get("sql"),
            "json_plan": plans[id_].get("plan")
        })

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for m in merged:
            out.write(json.dumps(m, ensure_ascii=False) + "\n")

    print(f"\nMerged records: {len(merged)}")
    print(f"Missing JSON plans: {missing}")
    print(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()