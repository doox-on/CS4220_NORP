import os
import json
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from tqdm import tqdm
import transformers

transformers.logging.set_verbosity_error()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "..", "data", "triplets_groq.jsonl")
NL2JSON_MODEL_PATH = os.path.join(BASE_DIR, "..", "finetune", "finetuned_nl2json_model")
JSON2SQL_MODEL_PATH = os.path.join(BASE_DIR, "..", "finetune", "finetuned_json2sql_model")
OUTPUT_FILE = os.path.join(BASE_DIR, "..", "data", "eval_ready", "nl_from_schema_ready2.jsonl")


print("Loading NL→JSON model...")
tok_nl2json = AutoTokenizer.from_pretrained(NL2JSON_MODEL_PATH, padding_side="left")
model_nl2json = AutoModelForCausalLM.from_pretrained(NL2JSON_MODEL_PATH, device_map="auto")
pipe_nl2json = pipeline(
    "text-generation",
    model=model_nl2json,
    tokenizer=tok_nl2json,
    batch_size=16,
    device_map="auto"
)

print("Loading JSON→SQL model...")
tok_json2sql = AutoTokenizer.from_pretrained(JSON2SQL_MODEL_PATH, padding_side="left")
model_json2sql = AutoModelForCausalLM.from_pretrained(JSON2SQL_MODEL_PATH, device_map="auto")
pipe_json2sql = pipeline(
    "text-generation",
    model=model_json2sql,
    tokenizer=tok_json2sql,
    batch_size=16,
    device_map="auto"
)

def batch_nl_to_json(nl_queries, max_json_tokens=512):
    prompts = [
        f"<s>[INSTRUCTION] Convert the following NL to JSON.\n[INPUT] {nl}\n[OUTPUT]"
        for nl in nl_queries
    ]
    outputs = pipe_nl2json(prompts, max_new_tokens=max_json_tokens)
    json_texts = []
    for output in outputs:
        generated_text = output[0]["generated_text"]
        json_text = generated_text.split("[OUTPUT]")[-1].split("</s>")[0].strip()
        json_texts.append(json_text)
    return json_texts


def batch_json_to_sql(json_texts, max_sql_tokens=256):
    prompts = [
        f"<s>[INSTRUCTION] Convert the following JSON to SQL.\n[INPUT] {json_text}\n[OUTPUT]"
        for json_text in json_texts
    ]
    outputs = pipe_json2sql(prompts, max_new_tokens=max_sql_tokens)
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

json_preds = []
print("Running NL→JSON pipeline (batch=16)")
for i in tqdm(range(0, len(nls), batch_size)):
    batch_nl = nls[i:i+batch_size]
    try:
        batch_json = batch_nl_to_json(batch_nl)
    except Exception as e:
        batch_json = [f"[ERROR] {e}"] * len(batch_nl)
    json_preds.extend(batch_json)

intermediate_json_path = OUTPUT_FILE + ".json_pred_tmp"
os.makedirs(os.path.dirname(intermediate_json_path), exist_ok=True)
with open(intermediate_json_path, "w", encoding="utf-8") as f_json_pred:
    for json_pred in json_preds:
        f_json_pred.write(json.dumps({"json_pred": json_pred}, ensure_ascii=False) + "\n")

pred_sqls = []
print("Running JSON→SQL pipeline (batch=16)")
for i in tqdm(range(0, len(json_preds), batch_size)):
    batch_json = json_preds[i:i+batch_size]
    try:
        batch_sql = batch_json_to_sql(batch_json)
    except Exception as e:
        batch_sql = [f"[ERROR] {e}"] * len(batch_json)
    pred_sqls.extend(batch_sql)

for nl, json_pred, pred_sql, gold_sql in zip(nls, json_preds, pred_sqls, gold_sqls):
    results.append({
        "nl": nl,
        "json_pred": json_pred,
        "pred_sql": pred_sql,
        "gold_sql": gold_sql
    })

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

if os.path.exists(intermediate_json_path):
    os.remove(intermediate_json_path)

print(f"\n Conversion complete — {len(results)} samples processed")
print(f"Results saved to: {OUTPUT_FILE}")
print("Evaluate it using query_comparator or query_tester.")