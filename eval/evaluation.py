import pandas as pd
import sqlite3
import json
import os
from datetime import datetime
import subprocess

DB_CACHE_PATH = "my_database.db"

CSV_PATH = "data/tables/demographic_race.csv"


# JSONL_PATH = "data/eval_ready/nl_to_json_sql_converted.jsonl"
JSONL_PATH = "data/eval_ready/nl_to_sql_v3.jsonl"

LOG_DIR = "eval/results"


def setup_database(csv_path):
    print(f"Loading CSV from: {csv_path}...")
    if os.path.exists(DB_CACHE_PATH):
        try:
            print(f"Using existing DB at {DB_CACHE_PATH}")
            conn = sqlite3.connect(DB_CACHE_PATH)
            # PRAGMA optimizations
            cursor = conn.cursor()
            cursor.execute("PRAGMA synchronous = OFF;")
            cursor.execute("PRAGMA journal_mode = MEMORY;")
            cursor.execute("PRAGMA temp_store = MEMORY;")
            cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
            # Move on-disk DB to in-memory DB for speed
            mem_conn = sqlite3.connect(":memory:")
            conn.backup(mem_conn)
            conn.close()
            return mem_conn
        except Exception as e:
            print(f"Failed to load existing DB ({e}), rebuilding from createDB.py...")
    else:
        print("DB not found. Running createDB.py to build DB...")
        subprocess.run(["python3", "createDB.py"], check=False)

        if os.path.exists(DB_CACHE_PATH):
            try:
                conn = sqlite3.connect(DB_CACHE_PATH)
                # PRAGMA optimizations
                cursor = conn.cursor()
                cursor.execute("PRAGMA synchronous = OFF;")
                cursor.execute("PRAGMA journal_mode = MEMORY;")
                cursor.execute("PRAGMA temp_store = MEMORY;")
                cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
                print("DB successfully built and loaded.")
                # Move on-disk DB to in-memory DB for speed
                mem_conn = sqlite3.connect(":memory:")
                conn.backup(mem_conn)
                conn.close()
                return mem_conn
            except Exception as e:
                print(f"DB built but failed to load: {e}. Falling back to internal build...")
    try:
        conn = sqlite3.connect("my_database.db")
        # PRAGMA optimizations
        cursor = conn.cursor()
        cursor.execute("PRAGMA synchronous = OFF;")
        cursor.execute("PRAGMA journal_mode = MEMORY;")
        cursor.execute("PRAGMA temp_store = MEMORY;")
        cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
        df = pd.read_csv(csv_path)
        df.columns = [c.strip().replace(" ", "_").replace("-", "_") for c in df.columns]
        df.to_sql("demographics", conn, if_exists="replace", index=False)
        mem_conn = sqlite3.connect(":memory:")
        conn.backup(mem_conn)
        conn.close()
        return mem_conn
    except Exception as e:
        print(f"DB Setup Failed: {e}")
        return None

def execute_and_compare(conn, pred_sql, gold_sql):
    cursor = conn.cursor()
    result = {
        "pred_res": None,
        "gold_res": None,
        "error": None,
        "match": False,
        "status": "FAIL"
    }

    try:
        cursor.execute(pred_sql)
        pred_res = cursor.fetchall()
        result["pred_res"] = pred_res

        cursor.execute(gold_sql)
        gold_res = cursor.fetchall()
        result["gold_res"] = gold_res

        if set(pred_res) == set(gold_res):
            result["match"] = True
            result["status"] = "PASS"
            
    except Exception as e:
        result["error"] = str(e)
        result["status"] = "ERROR"

    return result

def run_batch_evaluation():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{LOG_DIR}/eval_log_{timestamp}.txt"
    
    conn = setup_database(CSV_PATH)
    if not conn:
        return

    print(f"Starting Evaluation on {JSONL_PATH}...")
    print(f"Logging to: {log_filename}\n")

    total_cnt = 0
    pass_cnt = 0
    error_cnt = 0

    with open(JSONL_PATH, "r", encoding="utf-8") as f_in, \
         open(log_filename, "w", encoding="utf-8") as f_log:

        def log(msg):
            print(msg)
            f_log.write(msg + "\n")

        for line_idx, line in enumerate(f_in, 1):
            line = line.strip()
            if not line: continue
            
            try:
                data = json.loads(line)
                nl = data.get("nl", "")
                pred_sql = data.get("pred_sql", "")
                gold_sql = data.get("gold_sql", "")
                json_pred = data.get("json_pred", "(No JSON Pred)")
                
                gold_json = "(No Gold JSON)"
                merged_path = os.path.join("data", "merged_dataset.jsonl")
                try:
                    with open(merged_path, "r", encoding="utf-8") as mf:
                        for mline in mf:
                            mline = mline.strip()
                            if not mline:
                                continue
                            mitem = json.loads(mline)
                            if mitem.get("nl", "") == nl:
                                if "json_plan" in mitem:
                                    gold_json = mitem["json_plan"]
                                break
                except:
                    gold_json = "(Load Error)"
                

                total_cnt += 1
                
                res = execute_and_compare(conn, pred_sql, gold_sql)
                if res["match"]:
                    pass_cnt += 1
                if res["status"] == "ERROR":
                    error_cnt += 1

                log(f"[" + "="*20 + f" Test Case #{line_idx} " + "="*20 + "]")
                log(f"Question: {nl}")
                log(f"Pred SQL:  {pred_sql}")
                log(f"Gold SQL:  {gold_sql}")
                
                if res["error"]:
                    log(f"Execution Error: {res['error']}")
                else:
                    p_res_str = str(res['pred_res'])
                    g_res_str = str(res['gold_res'])
                    if len(p_res_str) > 200: p_res_str = p_res_str[:200] + "... (truncated)"
                    if len(g_res_str) > 200: g_res_str = g_res_str[:200] + "... (truncated)"
                    
                    log(f"Pred DB Result: {p_res_str}")
                    log(f"Gold DB Result: {g_res_str}")
                
                status_icon = "MATCH" if res["match"] else "X"
                log(f"result: {status_icon} {res['status']}\n")

            except json.JSONDecodeError:
                log(f"JSON Parsing Error on line {line_idx}\n")

        accuracy = (pass_cnt / total_cnt * 100) if total_cnt > 0 else 0
        summary = [
            "\n" + "="*50,
            "FINAL EVALUATION SUMMARY",
            "="*50,
            f"Total Test Cases: {total_cnt}",
            f"Passed:           {pass_cnt}",
            f"Failed:           {total_cnt - pass_cnt}",
            f"Errors (Syntax):  {error_cnt}",
            f"Execution Acc:    {accuracy:.2f}%",
            "="*50
        ]
        
        for s in summary:
            log(s)

    conn.close()

if __name__ == "__main__":
    run_batch_evaluation()