import os
import json
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from tqdm import tqdm
import transformers

transformers.logging.set_verbosity_error()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "..", "data", "triplets.jsonl")
NL2SQL_MODEL_PATH = os.path.join(BASE_DIR, "..", "finetune", "finetuned_nl2sql_model")
OUTPUT_FILE = os.path.join(BASE_DIR, "..", "data", "eval_ready", "nl_to_sql_predictions.jsonl")

print("Loading NL→SQL model...")
tok_nl2sql = AutoTokenizer.from_pretrained(NL2SQL_MODEL_PATH, padding_side="left")
model_nl2sql = AutoModelForCausalLM.from_pretrained(NL2SQL_MODEL_PATH, device_map="auto")
pipe_nl2sql = pipeline(
    "text-generation",
    model=model_nl2sql,
    tokenizer=tok_nl2sql,
    batch_size=16,
    device_map="auto"
)

def batch_nl_to_sql(nl_queries, max_sql_tokens=256):
    prompts = [
        f"<s>[INSTRUCTION] Convert the following NL to SQL.\n[INPUT] {nl}\n[OUTPUT]"
        for nl in nl_queries
    ]
    outputs = pipe_nl2sql(prompts, max_new_tokens=max_sql_tokens)
    sql_texts = []
    for output in outputs:
        generated_text = output[0]["generated_text"]
        sql_text = generated_text.split("[OUTPUT]")[-1].split("</s>")[0].strip()
        sql_texts.append(sql_text)
    return sql_texts

triplets = []
with open(DATA_FILE, "r", encoding="utf-8") as f:
    for line in f:
        try:
            triplets.append(json.loads(line))
        except json.JSONDecodeError:
            continue
print(f"Loaded {len(triplets)} triplets for inference")

batch_size = 16
results = []

nls = [item.get("nl", "").strip() for item in triplets]
gold_sqls = [item.get("sql", "").strip() for item in triplets]

pred_sqls = []
print("Running NL→SQL pipeline (batch=16)")
for i in tqdm(range(0, len(nls), batch_size)):
    batch_nl = nls[i:i+batch_size]
    try:
        batch_sql = batch_nl_to_sql(batch_nl)
    except Exception as e:
        batch_sql = [f"[ERROR] {e}"] * len(batch_nl)
    pred_sqls.extend(batch_sql)

for nl, pred_sql, gold_sql in zip(nls, pred_sqls, gold_sqls):
    results.append({
        "nl": nl,
        "pred_sql": pred_sql,
        "gold_sql": gold_sql
    })

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

print(f"\nConversion complete — {len(results)} samples processed")
print(f"Results saved to: {OUTPUT_FILE}")
print("Evaluate it using query_comparator or query_tester.")