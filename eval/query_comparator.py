import sqlite3
import time
import re
from typing import List, Dict, Tuple, Set, Optional

class QueryComparator:
    def __init__(self, database_path: str):
        """
        Initialize QueryComparator with database path.
        
        Args:
            database_path (str): Path to SQLite database
        """
        self.database_path = database_path
        self.conn = None

    def connect(self) -> None:
        """Establish database connection"""
        self.conn = sqlite3.connect(self.database_path)

    def disconnect(self) -> None:
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

    @staticmethod
    def levenshtein_distance(s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings"""
        m, n = len(s1), len(s2)
        dp = [[0]*(n+1) for _ in range(m+1)]
        
        for i in range(m+1):
            dp[i][0] = i
        for j in range(n+1):
            dp[0][j] = j
            
        for i in range(1, m+1):
            for j in range(1, n+1):
                cost = 0 if s1[i-1] == s2[j-1] else 1
                dp[i][j] = min(
                    dp[i-1][j] + 1,      # deletion
                    dp[i][j-1] + 1,      # insertion
                    dp[i-1][j-1] + cost  # substitution
                )
        return dp[m][n]

    @staticmethod
    def canonicalize(query: str) -> str:
        """Canonicalize SQL query for comparison"""
        query = re.sub(r'\s+', ' ', query).strip().lower()
        query = re.sub(r'(inner|left)\s+join', 'join', query)
        query = re.sub(r'\bwhere\b.*order by', 'order by', query)
        return query

    def run_sql_query(self, query: str) -> Tuple[list, float]:
        """Execute SQL query and return results with execution time"""
        try:
            start_time = time.time()
            cursor = self.conn.execute(query)
            results = cursor.fetchall()
            exec_time = time.time() - start_time
            return results, exec_time
        except Exception as e:
            return [], 0

    @staticmethod
    def output_similarity(pred: list, gt: list) -> float:
        """Compute output similarity using Jaccard similarity"""
        set_pred = set(pred)
        set_gt = set(gt)
        if not set_pred and not set_gt:
            return 1.0
        union = set_pred.union(set_gt)
        if not union:
            return 1.0
        intersection = set_pred.intersection(set_gt)
        return len(intersection) / len(union)

    def compare_queries(self, predicted_sql: str, ground_truth_sql: str) -> Dict:
        """Compare two SQL queries and return comprehensive metrics"""
        if not self.conn:
            self.connect()

        try:
            # Generate and execute predicted SQL
            start_time = time.time()
            pred_results, pred_exec_time = self.run_sql_query(predicted_sql)
            total_pred_time = time.time() - start_time

            # Execute ground truth SQL
            gt_results, gt_exec_time = self.run_sql_query(ground_truth_sql)

            # Calculate metrics
            pred_canonical = self.canonicalize(predicted_sql)
            gt_canonical = self.canonicalize(ground_truth_sql)
            
            is_exact_match = (pred_canonical == gt_canonical)
            is_execution_match = (pred_results == gt_results)

            dist = self.levenshtein_distance(pred_canonical, gt_canonical)
            max_len = max(len(pred_canonical), len(gt_canonical)) or 1
            structural_sim = 1 - (dist / max_len)

            out_sim = self.output_similarity(pred_results, gt_results)

            return {
                "exact_match": is_exact_match,
                "execution_match": is_execution_match,
                "structural_similarity": structural_sim,
                "output_similarity": out_sim,
                "execution_time": total_pred_time,
                "predicted_results": pred_results,
                "ground_truth_results": gt_results
            }

        except Exception as e:
            return {
                "error": str(e),
                "exact_match": False,
                "execution_match": False,
                "structural_similarity": 0.0,
                "output_similarity": 0.0,
                "execution_time": 0.0,
                "predicted_results": [],
                "ground_truth_results": []
            }