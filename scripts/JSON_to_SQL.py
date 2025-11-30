import json
import os
import re
import ast

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "..", "data", "eval_ready", "nl_to_json_v3.jsonl")
OUTPUT_FILE = os.path.join(BASE_DIR, "..", "data", "eval_ready", "nl_to_json_sql_converted.jsonl")
GOLD_DATA_FILE = os.path.join(BASE_DIR, "..", "data", "nl_json_sql.jsonl")

def clean_json_string(json_str):
    if not json_str: return None
    json_str = re.sub(r"```json\s*", "", json_str)
    json_str = re.sub(r"```\s*$", "", json_str)
    return json_str.strip()

def translate_json_to_sql(json_obj):
    try:
        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        col_to_agg_map = {}
        for item in json_obj.get("select", []):
            col = item.get("column")
            agg = item.get("agg")
            if col and agg and agg.upper() not in ["NONE", None]:
                col_to_agg_map[col] = agg.upper()


        select_parts = []
        for item in json_obj.get("select", []):
            col = item.get("column", "*")
            agg = item.get("agg")
            
            if agg and agg.upper() not in ["NONE", None]:
                expr = f"{agg.upper()}({col})"
            else:
                expr = col
            
            if item.get("alias"):
                expr += f" AS {item['alias']}"
            select_parts.append(expr)
        
        select_clause = "SELECT " + ", ".join(select_parts) if select_parts else "SELECT *"

        from_tables = json_obj.get("from", ["demographics"])
        from_clause = "FROM " + ", ".join(from_tables)

        where_parts = []
        having_parts = []
        
        agg_pattern = re.compile(r"(SUM|AVG|COUNT|MIN|MAX)\s*\(", re.IGNORECASE)
        all_conditions = json_obj.get("where", []) + json_obj.get("having", [])

        for cond in all_conditions:
            col = cond.get("column", "")
            op = cond.get("operator", "=")
            val = cond.get("value")

            if isinstance(val, str) and val.strip().startswith("[") and val.strip().endswith("]"):
                try:
                    parsed = ast.literal_eval(val)
                    if isinstance(parsed, list): val = parsed
                except: pass
            
            val_str = str(val)
            if op.upper() == "IN" and isinstance(val, list):
                formatted = [f"'{v}'" if isinstance(v, str) else str(v) for v in val]
                val_str = "(" + ",".join(formatted) + ")"
                clause_template = f"{{col}} IN {val_str}"
            else:
                clause_template = f"{{col}} {op} {val_str}"

            is_agg = False
            
            cond_agg = cond.get("agg")
            if cond_agg and cond_agg.upper() not in ["NONE", None]:
                is_agg = True
            elif agg_pattern.search(str(col)):
                is_agg = True
            elif isinstance(val, str) and agg_pattern.search(val):
                is_agg = True
            
            final_col_str = col
            if is_agg:
                if not agg_pattern.search(str(col)):
                    if cond_agg:
                         final_col_str = f"{cond_agg.upper()}({col})"
                    elif col in col_to_agg_map:
                        inferred_agg = col_to_agg_map[col]
                        final_col_str = f"{inferred_agg}({col})"

            
                having_parts.append(f"{final_col_str} {op} {val_str}")
            else:
                where_parts.append(f"{col} {op} {val_str}")

        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        group_by_clause = ""
        groups = json_obj.get("groupBy", [])
        if groups:
            group_by_clause = "GROUP BY " + ", ".join(groups)

        having_clause = "HAVING " + " AND ".join(having_parts) if having_parts else ""

        order_by_clause = ""
        orders = json_obj.get("orderBy", [])
        if isinstance(orders, dict): orders = [orders]
        order_parts = []
        for order in orders:
            c = order["column"]
            d = order.get("direction", "ASC").upper()
            order_parts.append(f"{c} {d}")
        if order_parts:
            order_by_clause = "ORDER BY " + ", ".join(order_parts)

        limit_clause = f"LIMIT {json_obj['limit']}" if json_obj.get("limit") is not None else ""

        clauses = [select_clause, from_clause, where_clause, group_by_clause, having_clause, order_by_clause, limit_clause]
        final_sql = " ".join([c for c in clauses if c])
        
        return final_sql

    except Exception as e:
        return None

def main():
    print(f"Reading from: {INPUT_FILE}")
    
    id_to_gold_sql = {}
    if os.path.exists(GOLD_DATA_FILE):
        with open(GOLD_DATA_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                try:
                    row = json.loads(line)
                    if "id" in row and "sql" in row:
                        id_to_gold_sql[row["id"]] = row["sql"]
                except: continue

    results = []
    
    if not os.path.exists(INPUT_FILE):
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            try:
                data = json.loads(line)
            except: continue
            
            gen_json_str = data.get("pred_json", "")
            translated_sql = None
            
            if gen_json_str:
                clean_str = clean_json_string(gen_json_str)
                try:
                    parsed_json = json.loads(clean_str)
                    translated_sql = translate_json_to_sql(parsed_json)
                except:
                    translated_sql = "Error"
            
            gold_sql = data.get("gold_sql") or data.get("sql")
            if not gold_sql:
                gold_sql = id_to_gold_sql.get(data.get("id"))

            results.append({
                "id": data.get("id"),
                "nl": data.get("nl"),
                "gold_sql": gold_sql,
                "pred_sql": translated_sql
            })
            
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            
    print(f"Translation Complete. Saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()