import os
import json
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from tqdm import tqdm
import transformers

transformers.logging.set_verbosity_error()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "..", "data", "nl_json_sql.jsonl")
NL2SQL_MODEL_PATH = os.path.join(BASE_DIR, "..", "finetune", "finetuned_nl2sql_model_v3")
OUTPUT_FILE = os.path.join(BASE_DIR, "..", "data", "eval_ready", "nl_to_sql_v3.jsonl")

BATCH_SIZE = 64
USE_FLASH_ATTN = False 

DB_SCHEMA = """
Table: demographics
Columns:
- year (INTEGER)
- id (TEXT)
- zipcode (TEXT)
- race_total_population (INTEGER)
- one_race (INTEGER)
- two_or_more_races (INTEGER)
- white (INTEGER)
- black (INTEGER)
- american_indian_and_alaska_native (INTEGER)
- asian (INTEGER)
- native_hawaiian_and_other_pacific_islander (INTEGER)
- some_other_race (INTEGER)
- hispanic_or_latino_total (INTEGER)
- hispanic_or_latino (INTEGER)
- not_hispanic_or_latino (INTEGER)

[Rules]
1. If the user asks for "total" across a category (like zipcode), use GROUP BY and SUM().
2. "hispanic_or_latino_total" implies the total population base, NOT the count of hispanic people. Use "hispanic_or_latino" for the count.
3. If the user asks for "counts" of a specific race, just SELECT the column (e.g., 'white'), DO NOT use COUNT() function unless asking for 'number of records'.
""".strip()

print(f"Loading NL→SQL model from: {NL2SQL_MODEL_PATH}")

tokenizer = AutoTokenizer.from_pretrained(NL2SQL_MODEL_PATH, padding_side="left")

model_kwargs = {
    "device_map": "auto",
    "torch_dtype": torch.bfloat16,
}

if USE_FLASH_ATTN:
    model_kwargs["attn_implementation"] = "flash_attention_2"

try:
    model = AutoModelForCausalLM.from_pretrained(NL2SQL_MODEL_PATH, **model_kwargs)
except Exception as e:
    print(f"Warning: Model loading failed with specific kwargs, falling back to default. Error: {e}")
    if "attn_implementation" in model_kwargs:
        del model_kwargs["attn_implementation"]
    model = AutoModelForCausalLM.from_pretrained(NL2SQL_MODEL_PATH, **model_kwargs)

pipe = pipeline(
    "text-generation",
    model=model,
    tokenizer=tokenizer,
    batch_size=BATCH_SIZE,
    pad_token_id=tokenizer.eos_token_id
)

def build_prompt(nl_query):
    return (
        f"<s>[INSTRUCTION] Convert the following NL to SQL based on the schema provided.\n"
        f"[SCHEMA] {DB_SCHEMA}\n"
        f"[INPUT] {nl_query}\n"
        f"[OUTPUT]"
    )

def batch_inference(nl_queries, max_new_tokens=512):
    prompts = [build_prompt(nl) for nl in nl_queries]
    
    outputs = pipe(prompts, max_new_tokens=max_new_tokens, do_sample=False)
    
    generated_sqls = []
    for output in outputs:
        full_text = output[0]["generated_text"]
        try:
            generated_text = full_text.split("[OUTPUT]")[-1].split("</s>")[0].strip()
        except IndexError:
            generated_text = full_text
        generated_sqls.append(generated_text)
        
    return generated_sqls

def main():
    triplets = []
    print(f"Reading data from {DATA_FILE}...")
    
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                triplets.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    
    print(f"Loaded {len(triplets)} samples.")
    
    nls = [item.get("nl", "").strip() for item in triplets]
    
    pred_sqls = []
    
    print(f"Running Inference on H100 (Batch Size: {BATCH_SIZE})...")
    
    for i in tqdm(range(0, len(nls), BATCH_SIZE)):
        batch_nl = nls[i : i + BATCH_SIZE]
        try:
            batch_result = batch_inference(batch_nl)
        except Exception as e:
            print(f"Error in batch {i}: {e}")
            batch_result = ["ERROR"] * len(batch_nl)
        pred_sqls.extend(batch_result)
        
    results = []
    for item, pred_sql in zip(triplets, pred_sqls):
        results.append({
            "id": item.get("id"),
            "nl": item.get("nl"),
            "gold_sql": item.get("sql"),
            "pred_sql": pred_sql
        })
        
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            
    print(f"\nInference Complete. Results saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()