import json
import sqlite3
import os

JSONL_FILE_PATH = "data/nl_sql.jsonl"
DB_PATH = "my_database.db"

def validate_sqls(jsonl_path, db_path):
    if not os.path.exists(db_path):
        print(f"Warning: '{db_path}' Cannot find the file")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f" Validation start: {jsonl_path}")

        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    sql_query = data.get("sql")

                    if not sql_query:
                        print(f"[SKIP] Line {line_number}: no sql.")
                        continue

                    try:
                        cursor.execute(sql_query)
                        
                        print(f"[VALID] Line {line_number}: {sql_query}")
                        
                    except sqlite3.Error as e:
                        print(f"[INVALID] Line {line_number}: {sql_query}")
                        print(f"  Error: {e}")

                except json.JSONDecodeError:
                    print(f"[ERROR] Line {line_number}: ")
                
                finally:
                    conn.rollback()

    except Exception as e:
        print(f"error!: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
        print("--- done ---")

if __name__ == "__main__":
    validate_sqls(JSONL_FILE_PATH, DB_PATH)