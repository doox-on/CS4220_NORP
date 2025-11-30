import json
import sqlglot
from sqlglot import exp


INPUT_FILE = "data/nl_sql.jsonl" 
OUTPUT_FILE = "data/nl_json_sql.jsonl"

def parse_sql_to_light_json(sql_query):
    try:
        parsed = sqlglot.parse_one(sql_query)
    except Exception as e:
        print(f"Error parsing SQL: {sql_query} | {e}")
        return None

    light_sql = {
        "select": [],
        "from": [],
        "where": [],
        "groupBy": [],
        "orderBy": [],
        "limit": None,
        "having": []
    }

    for table in parsed.find_all(exp.Table):
        light_sql["from"].append(table.name)

    for expression in parsed.find_all(exp.Select):
        for col_exp in expression.expressions:
            col_info = {"column": None, "agg": None, "alias": None}
            
            if isinstance(col_exp, exp.Alias):
                col_info["alias"] = col_exp.alias
                child = col_exp.this
            else:
                child = col_exp

            if isinstance(child, (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)):
                func_name = child.sql().split('(')[0].upper()
                col_info["agg"] = func_name

                if isinstance(child.this, exp.Star):
                    col_info["column"] = "*"
                elif child.this is None:
                    col_info["column"] = "*"
                else:
                    if hasattr(child.this, "name") and child.this.name:
                        col_info["column"] = child.this.name
                    else:
                        col_info["column"] = child.this.sql()
                    
            elif isinstance(child, exp.Column):
                col_info["column"] = child.name
            else:
                col_info["column"] = child.sql()

            light_sql["select"].append(col_info)

    if parsed.find(exp.Where):
        where_expression = parsed.find(exp.Where).this
        
        conditions = []
        
        def collect_conditions(node):
            if isinstance(node, exp.And):
                collect_conditions(node.left)
                collect_conditions(node.right)
            else:
                conditions.append(node)

        collect_conditions(where_expression)

        for cond in conditions:
            if isinstance(cond, (exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ, exp.Like)):
                operator_map = {
                    exp.EQ: "=", exp.GT: ">", exp.LT: "<", 
                    exp.GTE: ">=", exp.LTE: "<=", exp.NEQ: "!=", exp.Like: "LIKE"
                }
                
                val_node = cond.expression
                if isinstance(val_node, exp.Literal):
                    val = val_node.name if val_node.is_string else val_node.this
                    if val_node.is_string:
                        val = f"'{val}'"
                else:
                    val = val_node.sql()

                light_sql["where"].append({
                    "column": cond.this.name,
                    "operator": operator_map.get(type(cond), "UNKNOWN"),
                    "value": val
                })
            elif isinstance(cond, exp.In):
                light_sql["where"].append({
                    "column": cond.this.name,
                    "operator": "IN",
                    "value": str([e.name for e in cond.args['expressions']])
                })
            elif isinstance(cond, exp.Between):
                light_sql["where"].append({
                    "column": cond.this.name,
                    "operator": "BETWEEN",
                    "value": f"{cond.args['low'].name} AND {cond.args['high'].name}"
                })

    if parsed.find(exp.Group):
        for group in parsed.find(exp.Group).expressions:
            light_sql["groupBy"].append(group.name)

    if parsed.find(exp.Order):
        for order in parsed.find(exp.Order).expressions:
            light_sql["orderBy"].append({
                "column": order.this.name,
                "direction": "DESC" if order.args.get("desc") else "ASC"
            })

    if parsed.find(exp.Limit):
        light_sql["limit"] = int(parsed.find(exp.Limit).expression.this)
        
    if parsed.find(exp.Having):
        having_expression = parsed.find(exp.Having).this
        
        conditions = []
        def collect_conditions(node):
            if isinstance(node, exp.And):
                collect_conditions(node.left)
                collect_conditions(node.right)
            else:
                conditions.append(node)
        collect_conditions(having_expression)

        for cond in conditions:
            if isinstance(cond, (exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ, exp.Like)):
                operator_map = {
                    exp.EQ: "=", exp.GT: ">", exp.LT: "<", 
                    exp.GTE: ">=", exp.LTE: "<=", exp.NEQ: "!=", exp.Like: "LIKE"
                }
                
                left = cond.this
                col_name = left.sql()
                agg_func = None
                
                if isinstance(left, (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)):
                    agg_func = left.sql().split('(')[0].upper()
                    
                    if isinstance(left, exp.Count):
                        if isinstance(left.this, exp.Star) or left.this is None:
                            col_name = "*"
                        elif hasattr(left.this, "name") and left.this.name:
                            col_name = left.this.name
                        else:
                            col_name = left.this.sql()
                    else:
                        if hasattr(left.this, "name") and left.this.name:
                            col_name = left.this.name
                        elif left.this:
                            col_name = left.this.sql()
                            
                elif isinstance(left, exp.Column):
                    col_name = left.name

                val_node = cond.expression
                if isinstance(val_node, exp.Literal):
                    val = val_node.name if val_node.is_string else val_node.this
                    if val_node.is_string:
                        val = f"'{val}'"
                else:
                    val = val_node.sql()

                light_sql["having"].append({
                    "column": col_name,
                    "agg": agg_func,
                    "operator": operator_map.get(type(cond), "UNKNOWN"),
                    "value": val
                })
    return light_sql

def main():
    print(f"Starting conversion: {INPUT_FILE} -> {OUTPUT_FILE}")
    
    success_count = 0
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as fin, \
         open(OUTPUT_FILE, 'w', encoding='utf-8') as fout:
        
        for line in fin:
            if not line.strip(): continue
            
            data = json.loads(line)
            sql = data.get("sql")
            
            if sql:
                json_label = parse_sql_to_light_json(sql)
                
                if json_label:
                    data["json_label"] = json_label
                    fout.write(json.dumps(data, ensure_ascii=False) + "\n")
                    success_count += 1
                else:
                    print(f"Skipping ID {data.get('id')}: Parsing failed.")
            
    print(f"Done! Successfully processed {success_count} records.")

if __name__ == "__main__":
    main()