import json
import re

class BestEffortJSON2SQLInterpreter:
    def __init__(self):
        self.OP_MAP = {
            "SCAN": ["TableScan", "Scan", "Table Scan", "Source"],
            "FILTER": ["Filter", "Selection", "Where", "Having"],
            "PROJECT": ["Projection", "Project", "Select"],
            "AGG": ["Aggregation", "Aggregate", "GroupBy", "Grouping"],
            "SORT": ["Sort", "OrderBy", "Order By"],
            "LIMIT": ["Limit", "Top"],
            "MATH": ["Math", "Arithmetic", "Ratio"],
            "WINDOW": ["Window", "WindowFunction"]
        }

    def convert(self, json_plan):
        if isinstance(json_plan, str):
            try:
                if "[" in json_plan and "]" in json_plan:
                    match = re.search(r"\{.*\}", json_plan, re.DOTALL)
                    if match: json_plan = match.group(0)
                root = json.loads(json_plan)
            except:
                return "[INVALID_JSON_PARSING]"
        else:
            root = json_plan

        ctx = {
            "table": "demographics",
            "columns": [],     
            "filters": [],
            "groupby": [],
            "orderby": [],
            "limit": None,
            "aliases": {},      
            "windows": []     
        }

        try:
            self._traverse(root, ctx)
            return self._build_sql(ctx)
        except Exception as e:
            return f"Error: {str(e)}"

    def _get_val(self, details, keys, default=None):
        if not details: return default
        details_lower = {k.lower(): v for k, v in details.items()}
        for k in keys:
            if k.lower() in details_lower: return details_lower[k.lower()]
        return default

    def _parse_alias(self, expression):
        """ 'Expr AS Alias' 형태의 문자열에서 별칭과 수식을 분리 """
        match = re.search(r"(?i)(.*)\s+AS\s+([\w_]+)\s*$", expression)
        if match:
            expr = match.group(1).strip()
            alias = match.group(2).strip()
            return alias, expr
        return None, expression

    def _traverse(self, node, ctx):
        if not node: return

        raw_op = node.get("operation", "").upper()
        details = node.get("details", {})
        children = node.get("children", [])

        for child in children:
            self._traverse(child, ctx)

        if any(x.upper() == raw_op for x in self.OP_MAP["SCAN"]):
            t = self._get_val(details, ["table", "table_name"])
            if t: ctx["table"] = t

        elif any(x.upper() == raw_op for x in self.OP_MAP["FILTER"]):
            cond = self._get_val(details, ["condition", "filter"])
            if cond: ctx["filters"].append(cond)

        elif any(x.upper() == raw_op for x in self.OP_MAP["MATH"]):
            expr = self._get_val(details, ["expression", "formula"])
            alias = self._get_val(details, ["alias", "name"])
            
            if expr and alias:
                ctx["aliases"][alias] = expr
                ctx["columns"].append(f"{expr} AS {alias}")

        elif any(x.upper() == raw_op for x in self.OP_MAP["AGG"]):
            aggs = self._get_val(details, ["aggregates", "aggs"])
            if aggs:
                if isinstance(aggs, str): aggs = [aggs]
                for agg_str in aggs:
                    alias, expr = self._parse_alias(agg_str)
                    if alias:
                        ctx["aliases"][alias] = expr
                    ctx["columns"].append(agg_str)

            agg_type = self._get_val(details, ["type", "aggregationType"])
            target = self._get_val(details, ["target", "column"])
            alias = self._get_val(details, ["alias"]) # 혹시 alias 키가 따로 있다면

            if agg_type and target:
                expr = f"{agg_type}({target})"
                if alias:
                    ctx["aliases"][alias] = expr
                    ctx["columns"].append(f"{expr} AS {alias}")
                else:
                    ctx["columns"].append(expr)

            groups = self._get_val(details, ["groupby", "group_by"])
            if groups:
                if isinstance(groups, str): groups = [groups]
                ctx["groupby"].extend(groups)

        elif any(x.upper() == raw_op for x in self.OP_MAP["PROJECT"]):
            cols = self._get_val(details, ["columns", "projection", "target"])
            if cols:
                if isinstance(cols, str):
                    cols = [cols]

                expanded_cols = []
                remaining_windows = list(ctx.get("windows", []))

                for c in cols:
                    if c in ctx["aliases"]:
                        expanded_cols.append(f"{ctx['aliases'][c]} AS {c}")
                    elif remaining_windows and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", str(c)):
                        window_expr = remaining_windows.pop(0)
                        expanded_cols.append(f"{window_expr} AS {c}")
                    else:
                        expanded_cols.append(c)

                ctx["columns"] = expanded_cols

            hidden_filters = self._get_val(details, ["filters", "where"])
            if isinstance(hidden_filters, dict):
                for k, v in hidden_filters.items():
                    val = f"'{v}'" if isinstance(v, str) else v
                    ctx["filters"].append(f"{k} = {val}")

        elif any(x.upper() == raw_op for x in self.OP_MAP["WINDOW"]):

            func = self._get_val(details, ["function"]) or ""
            partition = (self._get_val(details, ["partition"]) or "").strip()
            order_clause = (self._get_val(details, ["order"]) or "").strip()

            if func:
                window_expr = func.strip() + " OVER ("
                inner_parts = []
                if partition:
                    inner_parts.append(partition)
                if order_clause:
                    inner_parts.append(order_clause)
                window_expr += " ".join(inner_parts) + ")"
                ctx["windows"].append(window_expr)

        elif any(x.upper() == raw_op for x in self.OP_MAP["SORT"]):
            full_order = self._get_val(details, ["order_by"])
            if full_order:
                ctx["orderby"].append(full_order)
            else:
                tgt = self._get_val(details, ["column", "target"])
                direction = self._get_val(details, ["order"], "ASC")
                if tgt:
                    ctx["orderby"].append(f"{tgt} {direction}")

        elif any(x.upper() == raw_op for x in self.OP_MAP["LIMIT"]):
            cnt = self._get_val(details, ["count", "limit"])
            if cnt is not None:
                ctx["limit"] = cnt

    def _build_sql(self, ctx):
        seen = set()
        final_cols = []
        
        candidates = ctx["columns"] if ctx["columns"] else ["*"]
        
        for c in candidates:
            if c not in seen:
                seen.add(c)
                final_cols.append(c)

        cols_str = ", ".join(final_cols)
        
        sql = f"SELECT {cols_str} FROM {ctx['table']}"

        if ctx["filters"]:
            sql += " WHERE " + " AND ".join(ctx["filters"])
        if ctx["groupby"]:
            sql += " GROUP BY " + ", ".join(ctx["groupby"])
        if ctx["orderby"]:
            sql += " ORDER BY " + ", ".join(ctx["orderby"])
        if ctx["limit"]:
            sql += f" LIMIT {ctx['limit']}"

        return sql + ";"