import os
import json
import time
from tqdm import tqdm
from groq import Groq


MODEL_NAME = "llama-3.1-8b-instant"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

OUTPUT_FILE = os.path.join(BASE_DIR, "data", "generated", "nl_from_schema.jsonl")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

client = Groq(api_key=os.getenv("GROQ_API_KEY", ""))


SCHEMA = """
CREATE TABLE demographics (
    year INT,
    id TEXT,
    zipcode TEXT,
    race_total_population INT,
    one_race INT,
    two_or_more_races INT,
    white INT,
    black INT,
    american_indian_and_alaska_native INT,
    asian INT,
    native_hawaiian_and_other_pacific_islander INT,
    some_other_race INT,
    hispanic_or_latino_total INT,
    hispanic_or_latino INT,
    not_hispanic_or_latino INT
);
"""

SYSTEM_PROMPT = f"""
You are a data scientist who generates realistic English questions for SQL queries.

Database schema:
{SCHEMA}

Your task:
- Write short, clear natural language questions (NLQs) that a human might ask about this table.
- Each question should relate directly to the columns in the schema.
- Vary the question type (aggregation, filtering, comparison, etc.).
- Do not output SQL.
- Keep each question under 25 words.
- Return the results as a numbered list, one question per line.
"""

def generate_nl_batch(batch_id: int, batch_size: int = 16):
    user_prompt = f"Generate {batch_size} new, unique natural language questions about the demographics table."

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT.strip()},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.8,
            max_tokens=800, 
        )

        content = response.choices[0].message.content
        questions = []

        for line in content.split("\n"):
            line = line.strip().lstrip("-").lstrip("*").lstrip("0123456789. ").strip()
            if len(line) > 5:
                questions.append(line)

        questions = list(dict.fromkeys(questions))

        print(f"Batch {batch_id}: Generated {len(questions)} NL questions")
        return questions

    except Exception as e:
        print(f"[!] Error in batch {batch_id}: {e}. Retrying in 2s...")
        time.sleep(2)
        return generate_nl_batch(batch_id, batch_size)



TARGET_COUNT = 500
BATCH_SIZE = 16

all_questions = []

for batch_id in tqdm(range(1, (TARGET_COUNT // BATCH_SIZE) + 2), desc="Generating NLs"):
    new_qs = generate_nl_batch(batch_id, batch_size=BATCH_SIZE)
    all_questions.extend(new_qs)

    # 중간 저장
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for q in all_questions:
            f.write(json.dumps({"nl": q}, ensure_ascii=False) + "\n")

    if len(all_questions) >= TARGET_COUNT:
        break

print(f"\nDone! Generated {len(all_questions)} NL questions.")
print(f"Saved to: {OUTPUT_FILE}")
print("Each line is a JSON object like: {'nl': 'What is the total population in 2019?'}")