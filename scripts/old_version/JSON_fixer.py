import json
import re
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from tqdm import tqdm

def extract_json_block(text: str) -> str:

    if "```" in text:
        if "```json" in text:
            text = text.split("```json", 1)[-1]
        text = text.split("```", 1)[-1]
    
    text = text.strip()
    

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0).strip()
    
    raise ValueError("No JSON object found in model output.")

# NOTE:

class IRValidator:

    REQUIRED_KEYS = {"operation", "details"}
    
    OP_DETAILS_SCHEMA = {
        "Scan": ["table"],
        "Filter": ["condition"],
        "Projection": ["columns"],
        "Aggregation": ["aggregates", "groupby"],
        "Window": ["function"],
        "Sort": ["order_by"],
        "Limit": ["count"],
        "Math": ["expression", "alias"]
    }

    @staticmethod
    def validate(node):
        if not isinstance(node, dict):
            raise ValueError("Node must be a JSON object (dict).")

        if not IRValidator.REQUIRED_KEYS.issubset(node.keys()):
            raise ValueError(f"Missing required keys: {IRValidator.REQUIRED_KEYS - node.keys()}")

        op = node.get("operation")
        details = node.get("details")
        children = node.get("children", [])

        if not isinstance(op, str) or not op:
            raise ValueError("Operation name must be a non-empty string.")
        
        if op not in IRValidator.OP_DETAILS_SCHEMA:
            raise ValueError(
                f"Unknown operation '{op}'. Expected one of: {list(IRValidator.OP_DETAILS_SCHEMA.keys())}"
            )

        if not isinstance(details, dict):
            raise ValueError(f"'{op}' details must be a dictionary.")
        
        if not isinstance(children, list):
            raise ValueError(f"'{op}' children must be a list.")

        required_detail_keys = IRValidator.OP_DETAILS_SCHEMA[op]

        found = False
        for k in required_detail_keys:
            if k in details:
                found = True
                break
        if not found:
            raise ValueError(
                f"Operation '{op}' details missing required keys. "
                f"Expected one of: {required_detail_keys}. Got: {list(details.keys())}"
            )

        for child in children:
            IRValidator.validate(child)

        return True

class LocalLLM:
    def __init__(self, model_path):
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_path)
            self.model = AutoModelForCausalLM.from_pretrained(
                model_path, 
                torch_dtype=torch.float16, 
                device_map="auto"
            )
        except:
            self.model = None

    def generate(self, prompt):
        if self.model is None:
            return '{"operation": "Scan", "details": {"table": "demographics"}, "children": []}'

        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        with torch.no_grad():
            outputs = self.model.generate(
                **inputs, 
                max_new_tokens=1024,
                temperature=0.2,
                do_sample=True,
                pad_token_id=self.tokenizer.eos_token_id
            )
        return self.tokenizer.decode(outputs[0], skip_special_tokens=True)

def process_single_item(llm, nl_query):
    messages = f"""### Instruction:
Convert the Natural Language Query into a valid JSON Execution Plan.
Strictly follow the schema: {{ "operation": "...", "details": {{...}}, "children": [...] }}

### Input:
{nl_query}

### Response:
"""
    
    current_prompt = messages

    for attempt in range(MAX_RETRIES):
        output_text = llm.generate(current_prompt)
        
        if "### Response:" in output_text:
            generated_part = output_text.split("### Response:")[-1].strip()
        else:
            generated_part = output_text

        try:
            json_str = extract_json_block(generated_part)
            
            json_obj = json.loads(json_str)
            
            IRValidator.validate(json_obj)
            
            return json_obj, True

        except Exception as e:
            error_msg = str(e)
            
            truncated = generated_part
            if len(truncated) > 800:
                truncated = truncated[:800] + "...(truncated)"
            
            current_prompt += (
                f"\n### Last Attempted JSON (invalid):\n{truncated}\n"
                f"\n### Error Feedback:\n"
                f"Your previous JSON does NOT follow the required IR schema.\n"
                f"Error: {error_msg}\n"
                f"Please output ONLY a corrected JSON object, with no extra text.\n"
                f"\n### Response:\n"
            )

    return None, False

def main():
    llm = LocalLLM(MODEL_PATH)
    
    data_list = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                data_list.append(json.loads(line))
    
    success_cnt = 0
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f_out:
        for item in tqdm(data_list):
            nl = item.get("nl")
            if not nl: continue
            
            valid_json, success = process_single_item(llm, nl)
            
            if success:
                result_entry = {
                    "id": item.get("id"),
                    "nl": nl,
                    "gold_sql": item.get("sql"),
                    "json_pred": valid_json
                }
                f_out.write(json.dumps(result_entry, ensure_ascii=False) + "\n")
                success_cnt += 1
            else:
                pass

if __name__ == "__main__":
    main()