CS 4220 Jungwoo 
NORP

from pathlib import Path

readme_content = """# NORP Data Integration & NL2SQL Pipeline

This project implements a complete pipeline for generating **SQL–JSON–NL triplets** from a demographic dataset.  
It uses automated SQL generation, LLM-based interpretation (via Groq API), and dataset merging for NL2SQL model training.

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
source .venv/bin/activate        # (Mac/Linux)
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

## 2. Environment Variables

Create a `.env` file in the project root directory (`NORP/`) to store your API key safely:
```
GROQ_API_KEY=your_groq_api_key_here
```

This ensures your key is **not pushed to GitHub** and remains secure.

---

## 3. Folder Structure

```
NORP/
│
├── data/
│   ├── raw_sql/           # Auto-generated SQL queries
│   ├── json_plans/        # SQL → JSON execution plans
│   ├── nl_plans/          # SQL → Natural language questions
│   └── merged_dataset.jsonl  # Final combined triplet dataset
│
├── scripts/
│   ├── random_query.py    # Generate 500 random SQL queries
│   ├── SQL-to-JSON.py     # Convert SQL → JSON (execution plan)
│   ├── SQL-to-NL.py       # Convert SQL → Natural Language
│   └── make_triplets.py   # Merge SQL, JSON, and NL into triplets
│
└── .env                   # (not pushed to GitHub)
```

---

## ⚙️ 4. How to Run Each Step

### 4.1 Generate Random SQL Queries
Run the following script to create 500 SQL queries from your CSV dataset:
```bash
python scripts/random_query.py
```
This will scan your dataset in `data/tables/`, infer column types automatically,  
and save queries to:
```
data/raw_sql/random_sql_queries.txt
```

---

### 4.2 Convert SQL → JSON Execution Plans
Use the Groq LLM to convert SQL queries into structured execution plans:
```bash
python scripts/SQL-to-JSON.py
```
Each SQL file will be processed and saved as:
```
data/json_plans/query_xxx.json
```

---

### 4.3 Convert SQL → Natural Language (NL)
Next, translate SQL queries into plain English questions:
```bash
python scripts/SQL-to-NL.py
```
Outputs will be saved to:
```
data/nl_plans/nl_generated.txt
```

Example output format:
```
SELECT AVG(hispanic_or_latino) FROM demographics;
→ What is the average percentage of people who identify as Hispanic or Latino?
```

---

### 4.4 Merge All Triplets (NL + SQL + JSON)
Finally, merge all valid files into a single dataset:
```bash
python scripts/make_triplets.py
```

This script automatically skips invalid or empty JSON files.  
Result will be stored as:
```
data/merged_dataset.jsonl
```

---

## 5. Example of a Valid Triplet
Each line in `merged_dataset.jsonl` contains one structured triplet:
```json
{
  "id": 3,
  "nl": "What is the average percentage of people who identify as Hispanic or Latino?",
  "sql": "SELECT AVG(hispanic_or_latino) FROM demographics;",
  "json_plan": {
    "operation": "Aggregation",
    "details": { "aggregation_target": "hispanic_or_latino", "function": "AVG" },
    "children": [
      { "operation": "TableScan", "details": { "table_name": "demographics" }, "children": [] }
    ]
  }
}
```

---

## 6. Notes & Troubleshooting

- Make sure your `.env` file is **never committed** (add `.env` to `.gitignore`).
- You can test your Groq API key by running:
  ```bash
  python -c "from groq import Groq; print(Groq(api_key='your_key'))"
  ```
- If you see `Invalid JSON` errors during SQL-to-JSON generation, they will be automatically skipped in `make_triplets.py`.

---

## 👩‍💻 Authors
**Georgia Tech CS4220 / NORP Data Integration Team**  
> Project Lead: *Jungwoo Moon*  
> Dataset: *Demographic Race Data (2015–2019)*  
> Model: *Groq LLaMA 3.1–8B-Instant*  
> Repository: [CS4220_NORP](https://github.com/doox-on/CS4220_NORP)

---

## 🧾 License
This project is distributed under the **MIT License**.
"""

# Write to README.md
readme_path = Path("README.md")
readme_path.write_text(readme_content, encoding="utf-8")
readme_path