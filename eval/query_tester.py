import os
import json
import argparse
from datetime import datetime
from query_comparator import QueryComparator
from typing import List, Dict

class QueryTester:
    def __init__(self, database_path: str, predictions_path: str):
        """
        Initialize QueryTester with paths to database and predicted SQL results.

        Args:
            database_path (str): Path to SQLite database
            predictions_path (str): Path to JSONL file containing predictions
        """
        self.database_path = database_path
        self.predictions_path = predictions_path
        self.comparator = QueryComparator(database_path)
        self.test_data = []

    def load_jsonl_data(self) -> None:
        """Load test data from a JSONL file"""
        self.test_data = []
        with open(self.predictions_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    if "pred_sql" in obj and "gold_sql" in obj:
                        self.test_data.append({
                            "nlq": obj.get("nl", ""),
                            "pred_sql": obj["pred_sql"],
                            "gold_sql": obj["gold_sql"]
                        })
                except json.JSONDecodeError:
                    continue
        print(f"📂 Loaded {len(self.test_data)} test cases from {self.predictions_path}")

    def run_tests(self) -> Dict:
        """Run all tests and return aggregated metrics"""
        try:
            self.comparator.connect()
            total_tests = len(self.test_data)
            all_metrics = []

            for i, test_case in enumerate(self.test_data, start=1):
                nlq = test_case["nlq"]
                pred_sql = test_case["pred_sql"]
                gt_sql = test_case["gold_sql"]

                metrics = self.comparator.compare_queries(pred_sql, gt_sql)
                all_metrics.append(metrics)

                print(f"Test {i}:")
                print("  NLQ:", nlq)
                print("  Predicted SQL:", pred_sql)
                print("  Ground Truth SQL:", gt_sql)
                print(f"  Exact Match: {metrics['exact_match']}")
                print(f"  Execution Match: {metrics['execution_match']}")
                print(f"  Structural Similarity: {metrics['structural_similarity']:.3f}")
                print(f"  Output Similarity: {metrics['output_similarity']:.3f}")
                print(f"  Execution Time: {metrics['execution_time']:.3f} seconds\n")

            aggregate_metrics = {
                "exact_match": sum(m["exact_match"] for m in all_metrics) / total_tests,
                "execution_match": sum(m["execution_match"] for m in all_metrics) / total_tests,
                "avg_structural_sim": sum(m["structural_similarity"] for m in all_metrics) / total_tests,
                "avg_output_sim": sum(m["output_similarity"] for m in all_metrics) / total_tests,
                "avg_exec_time": sum(m["execution_time"] for m in all_metrics) / total_tests
            }

            return aggregate_metrics

        finally:
            self.comparator.disconnect()

    def print_aggregate_results(self, metrics: Dict) -> None:
        """Print aggregate test results"""
        print("\n===== Overall Results =====")
        print(f"Exact Match (EM): {metrics['exact_match']:.3f}")
        print(f"Execution Match (EX): {metrics['execution_match']:.3f}")
        print(f"Average Structural Similarity: {metrics['avg_structural_sim']:.3f}")
        print(f"Average Output Similarity: {metrics['avg_output_sim']:.3f}")
        print(f"Average Execution Time: {metrics['avg_exec_time']:.3f} seconds")




def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pred_file", type=str, required=True, help="Path to NL2SQL prediction JSONL file")
    parser.add_argument("--save_dir", type=str, default="eval/results", help="Directory to save text logs")
    args = parser.parse_args()

    os.makedirs(args.save_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(args.save_dir, f"query_eval_log_{timestamp}.txt")

    tester = QueryTester(
        database_path="my_database.db",
        predictions_path=args.pred_file
    )

    tester.load_jsonl_data()

    with open(log_path, "w", encoding="utf-8") as log_f:
        tester.comparator.connect()
        total_tests = len(tester.test_data)
        all_metrics = []

        for i, test_case in enumerate(tester.test_data, start=1):
            nlq = test_case["nlq"]
            pred_sql = test_case["pred_sql"]
            gt_sql = test_case["gold_sql"]

            metrics = tester.comparator.compare_queries(pred_sql, gt_sql)
            all_metrics.append(metrics)

            output_lines = [
                f"Test {i}:",
                f"  NLQ: {nlq}",
                f"  Predicted SQL: {pred_sql}",
                f"  Ground Truth SQL: {gt_sql}",
                f"  Exact Match: {metrics['exact_match']}",
                f"  Execution Match: {metrics['execution_match']}",
                f"  Structural Similarity: {metrics['structural_similarity']:.3f}",
                f"  Output Similarity: {metrics['output_similarity']:.3f}",
                f"  Execution Time: {metrics['execution_time']:.3f} seconds",
                "",
            ]

            for line in output_lines:
                print(line)
                log_f.write(line + "\n")

        tester.comparator.disconnect()

        total = len(all_metrics)
        summary = {
            "exact_match": sum(m['exact_match'] for m in all_metrics) / total,
            "execution_match": sum(m['execution_match'] for m in all_metrics) / total,
            "avg_structural_sim": sum(m['structural_similarity'] for m in all_metrics) / total,
            "avg_output_sim": sum(m['output_similarity'] for m in all_metrics) / total,
            "avg_exec_time": sum(m['execution_time'] for m in all_metrics) / total
        }

        summary_lines = [
            "",
            "===== Overall Results =====",
            f"Exact Match (EM): {summary['exact_match']:.3f}",
            f"Execution Match (EX): {summary['execution_match']:.3f}",
            f"Average Structural Similarity: {summary['avg_structural_sim']:.3f}",
            f"Average Output Similarity: {summary['avg_output_sim']:.3f}",
            f"Average Execution Time: {summary['avg_exec_time']:.3f} seconds",
        ]

        for line in summary_lines:
            print(line)
            log_f.write(line + "\n")

    print(f"\nDetailed log saved to: {log_path}")


if __name__ == "__main__":
    main()