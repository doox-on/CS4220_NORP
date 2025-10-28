import os
import csv
import random

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_FILE = os.path.join(BASE_DIR, "data", "tables", "demographic_race.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "raw_sql", "random_sql_queries.txt")
TABLE_NAME = "demographics"

def infer_type_from_value(value):
    value = value.strip()
    if not value or value.lower() == "nan":
        return "unknown" 
    try:
        int(value)
        return "int"
    except ValueError:
        try:
            float(value)
            return "float"
        except ValueError:
            return "str"

types = {}
zipcodes = set()
with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    
    types = {col: "unknown" for col in header}
    
    for i, row in enumerate(reader):
        if i >= 100:  
            break
        
        row_dict = dict(zip(header, row))
        
        if "zipcode" in row_dict and row_dict["zipcode"]:
            zipcodes.add(row_dict["zipcode"].replace("ZCTA5 ", "").strip())
        
        for col, val in row_dict.items():
            if types[col] == "str":
                continue
            
            val_type = infer_type_from_value(val)
            
            if val_type == "str":
                types[col] = "str"
            elif val_type == "float":
                if types[col] == "unknown" or types[col] == "int":
                    types[col] = "float"
            elif val_type == "int":
                if types[col] == "unknown":
                    types[col] = "int"

for col, t in types.items():
    if t == "unknown":
        types[col] = "str"

numeric_cols = [c for c, t in types.items() if t in ("int", "float")]
text_cols = [c for c, t in types.items() if t == "str"]
zipcodes = list(zipcodes)

if "year" in numeric_cols:
    numeric_cols.remove("year")

print("📊 Detected column types (100-row scan):")
for c, t in types.items():
    print(f"  - {c}: {t}")

if not numeric_cols:
    print("\n⚠️ ERROR: there's no num value in CSV")
    exit()
    
if not zipcodes:
    print("\n⚠️ ERROR: there's no Zipcode in CSV")



years = [2015, 2016, 2017, 2018, 2019] 
aggs = ["SUM", "AVG", "MAX", "MIN"]
orders = ["ASC", "DESC"]
N_values = [1, 5, 10, 20]

templates = [
    # (tag, template_string)
    ("num", "SELECT {agg}({col}) FROM {table};"),
    ("num", "SELECT {agg}({col}) FROM {table} WHERE year = {year};"),
    ("num", "SELECT {agg}({col}) FROM {table} WHERE zipcode = 'ZCTA5 {zip}';"),
    ("num", "SELECT zipcode FROM {table} WHERE {col} > {num};"),
    ("num", "SELECT zipcode FROM {table} WHERE {col} < {num} AND year = {year};"),
    ("num", "SELECT COUNT(*) FROM {table} WHERE {col} > {num} AND year = {year};"),
    ("num", "SELECT zipcode, {col} FROM {table} ORDER BY {col} {order} LIMIT {N};"),
    ("num", "SELECT zipcode, {col} FROM {table} WHERE year = {year} ORDER BY {col} {order} LIMIT {N};"),
    ("num", "SELECT year, {agg}({col}) FROM {table} GROUP BY year;"),
    ("num", "SELECT zipcode, {agg}({col}) FROM {table} GROUP BY zipcode;"),
    ("num", "SELECT year, {agg}({col}) AS agg_val FROM {table} GROUP BY year ORDER BY agg_val {order};"),
    ("num", "SELECT zipcode, {agg}({col}) AS agg_val FROM {table} GROUP BY zipcode ORDER BY agg_val {order} LIMIT {N};"),
    ("any", "SELECT {col} FROM {table} WHERE zipcode = 'ZCTA5 {zip}' AND year = {year};"),
    ("fixed", "SELECT DISTINCT year FROM {table};")
]

sql_queries = []
for _ in range(500):
    tag, t_string = random.choice(templates)
    
    params = {
        "table": TABLE_NAME,
        "year": random.choice(years),
        "zip": random.choice(zipcodes) if zipcodes else "30005", 
        "num": random.randint(100, 10000), 
        "agg": random.choice(aggs),
        "order": random.choice(orders),
        "N": random.choice(N_values),
        "col": random.choice(header)
    }

    if tag == "num":
        params["col"] = random.choice(numeric_cols)
    elif tag == "any":
        params["col"] = random.choice(header)

    try:
        sql = t_string.format_map(params)
        sql_queries.append(sql)
    except KeyError as e:
        print(f"템플릿 포맷 오류 발생: {e} | 템플릿: {t_string}")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for q in sql_queries:
        f.write(q + "\n")

print(f"\n Random generated 500 SQL queries are saved in {OUTPUT_FILE}")
