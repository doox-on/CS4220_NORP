import os
import json
import re
from groq import Groq

MODEL_NAME = "llama-3.1-8b-instant"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_SQL_FILE = os.path.join(BASE_DIR, "data", "raw_sql", "random_sql_queries.txt")
OUTPUT_JSON_DIR = os.path.join(BASE_DIR, "data", "json_plans")

os.makedirs(OUTPUT_JSON_DIR, exist_ok=True)

# Initialize Groq client
client = Groq(api_key="")


def generate_json_plan(sql_query: str):
    """Ask LLM to generate a structured execution plan in JSON."""
    prompt = f"""
    You are a database query planner assistant.

    Given the following SQL query, produce a *structured JSON representation*
    of its execution plan, as if explaining how a database would execute it.

    The JSON must include:
      - "operation": The main type (e.g., Projection, Filter, Join, Aggregation, Sort)
      - "details": Optional info (e.g., filter condition, join type, aggregation target)
      - "children": A list of sub-operations (recursive)

    Only return valid JSON. Do NOT add extra text or explanations.

    SQL Query:
    {sql_query}
    """

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        content = response.choices[0].message.content.strip()

        if content.startswith("```"):
            content = content.strip("`")               
            content = content.replace("json", "", 1)
            content = content.strip()
        
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            content = match.group(0)
    
        return json.loads(content)
    
    except Exception as e:
            return {"error": "Invalid JSON or LLM error", "message": str(e), "raw_output": content}



def main():
    if not os.path.exists(RAW_SQL_FILE):
        print(f"File not found: {RAW_SQL_FILE}")
        return

    with open(RAW_SQL_FILE, "r") as f:
        sql_queries = [line.strip() for line in f.readlines() if line.strip()]

    print(f"Generating JSON plans for {len(sql_queries)} SQL queries...")

    for i, sql_query in enumerate(sql_queries, start=1):
        plan = generate_json_plan(sql_query)

        out_path = os.path.join(OUTPUT_JSON_DIR, f"{i:04d}.json")
        with open(out_path, "w") as out_f:
            json.dump(plan, out_f, indent=2)

        print(f"[{i:03d}/{len(sql_queries)}] {sql_query[:60]}...")

    print("\n All SQL queries processed successfully!")


if __name__ == "__main__":
    main()