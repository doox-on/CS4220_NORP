## CS 4220 Jungwoo Moon
## Project: NORP

This project implements and evaluates a complete pipeline for generating SQL–JSON–NL triplets. It serves as a research artifact to compare two Text-to-SQL methodologies:

Direct SQL Generation (Baseline): Mapping Natural Language directly to SQL.

IR-based Generation (Experimental): Mapping Natural Language to an Intermediate Representation (JSON Execution Plan) before converting to SQL.

Inspired by the concept of Weld, this project utilizes automated SQL generation, LLM-based interpretation (via Groq API), and a custom dataset merging pipeline to investigate the trade-offs of structured intermediate representations..

---

## 1. Environment Setup

### 1.1 Clone the Repository
```bash
git clone https://github.com/doox-on/CS4220_NORP.git
cd CS4220_NORP
```

### 1.2 Create and Activate Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate   

# or on Windows:
# .venv\\Scripts\\activate
```

### 1.3 Install Dependencies
```bash
pip install -r requirements.txt
```

If you don’t have a `requirements.txt`, you can export it from your environment:
```bash
pip freeze > requirements.txt
```

---


## 2. How to Run Each Step

Run this prompt on Chat GPT or other high performance LLM 

```
### 1. Database Schema
Table: demographics
Columns:
- year (INTEGER): The census year (e.g., 2010, 2020)
- id (TEXT): Unique record identifier
- zipcode (TEXT): 5-digit zip code (treat as string)
- race_total_population (INTEGER): Total population count
- one_race (INTEGER): Population of one race
- two_or_more_races (INTEGER): Population of two or more races
- white (INTEGER)
- black (INTEGER)
- american_indian_and_alaska_native (INTEGER)
- asian (INTEGER)
- native_hawaiian_and_other_pacific_islander (INTEGER)
- some_other_race (INTEGER)
- hispanic_or_latino_total (INTEGER)
- hispanic_or_latino (INTEGER)
- not_hispanic_or_latino (INTEGER)

### 2. Constraints & Rules
1. **Valid SQLite Only:** Use standard SQLite syntax.
2. **Standard Analytics Only:**
   - Use: SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT.
   - Use Aggregates: SUM(), AVG(), MIN(), MAX(), COUNT().
   - **Avoid:** Complex arithmetic (e.g., percentages, ratios), nested subqueries, or joins. Keep the logic focused on retrieval and aggregation to ensure easy evaluation.
3. **Column Validity:** Use ONLY the columns listed above. Do not hallucinate new columns (e.g., do not use gender, state, city).
4. **Diversity is Critical:**
   - Mix simple lookups (e.g., "Find population of zipcode 90210").
   - Mix aggregations (e.g., "Total Asian population in 2022").
   - Mix groupings (e.g., "Average White population per year").
   - Mix filters (e.g., "Zipcodes where Black population > 1000").
   - Mix ordering (e.g., "Top 3 zipcodes with highest Hispanic population").
5. **Format:** Output strictly in **JSONL (JSON Lines)** format. One valid JSON object per line.

### 3. Output Format
{"id": <integer>, "nl": "<natural language question>", "sql": "<sqlite query>"}

### 4. Task
Generate **50** unique, diverse, and high-quality pairs starting from ID **1** (or continue from the last ID provided).
Ensure the Natural Language (nl) is varied (use synonyms like "Find", "Show", "List", "What is", "Count").
```

Save generated jsonl data into data/nl_sql.jsonl


---
### 2.2 Validate Generated SQL set
validated generated SQL
```bash
python scripts/validate.py
```
will check basic examination of SQL syntax. 

---

### 2.3 make 
Use the sqlglot to convert SQL queries into structured execution plans:

```bash
python scripts/generate_json.py
```
Each SQL file will be processed and saved as:
```
data/json_plans/nl_json_sql.json
```

---

### 2.4 Fintune Local models 
You need to login to hugging face and get access to Llama 3.1 before to do this


```bash
python finetune_nl_to_json.py
python finetune_nl_to_sql.py

```
will generate finetuned models 

---

### 2.5 Convert NL → SQL
```bash
python scripts/new_NL_to_SQL.py
```
Outputs will be saved to:
```
data/eval_ready/nl_to_sql_v3.jsonl
```

---

### 2.5 Convert NL → JSON
```bash
python scripts/new_NL_to_JSON.py
```
Outputs will be saved to:
```
data/eval_ready/nl_to_sql_v3.jsonl
```
---

### 2.6 Convert JSON → SQL
```bash
python scripts/JSON_to_SQL.py
```
Outputs will be saved to:
```
data/eval_ready/nl_to_sql_to_sql_converted.jsonl
```

---

### 2.7 Evaluation
```bash
python eval/evaluation.py
```
Outputs will be saved to:
```
eval/results/logs.txt
```
---

## 5. Example of results 
```txt
==================================================
FINAL EVALUATION SUMMARY
==================================================
Total Test Cases: 500
Passed:           376
Failed:           124
Errors (Syntax):  55
Execution Acc:    75.20%
==================================================

```

---

source /storage/ice1/9/8/jmoon318/venvs/norp_env/bin/activate

huggingface-cli login