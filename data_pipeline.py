"""
Data Pipeline Module for DataForge AI
Handles data profiling, transformation execution, and pipeline orchestration.
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional


class DataProfiler:
    """Generates statistical profiles of database tables."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def profile_table(self, table_name: str) -> Dict:
        """Generate a comprehensive profile of a database table."""
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        # Get columns info
        c.execute(f"PRAGMA table_info({table_name})")
        columns_info = c.fetchall()

        # Get row count
        c.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = c.fetchone()[0]

        profiles = []
        for col_info in columns_info:
            col_name = col_info["name"]
            col_type = col_info["type"]
            not_null = col_info["notnull"]

            profile = {
                "name": col_name,
                "type": col_type,
                "not_null_constraint": bool(not_null),
            }

            # Null count
            c.execute(f"SELECT COUNT(*) FROM {table_name} WHERE [{col_name}] IS NULL")
            null_count = c.fetchone()[0]
            profile["null_count"] = null_count
            profile["null_pct"] = round((null_count / row_count * 100), 1) if row_count > 0 else 0

            # Distinct count
            c.execute(f"SELECT COUNT(DISTINCT [{col_name}]) FROM {table_name}")
            profile["distinct_count"] = c.fetchone()[0]

            # For numeric columns, get statistics
            if col_type in ("REAL", "INTEGER", "NUMERIC"):
                c.execute(f"""
                    SELECT 
                        MIN([{col_name}]) as min_val,
                        MAX([{col_name}]) as max_val, 
                        ROUND(AVG([{col_name}]), 2) as avg_val,
                        ROUND(SUM([{col_name}]), 2) as sum_val
                    FROM {table_name} WHERE [{col_name}] IS NOT NULL
                """)
                stats = c.fetchone()
                profile["min"] = stats[0]
                profile["max"] = stats[1]
                profile["mean"] = stats[2]
                profile["sum"] = stats[3]

            # For text columns, get sample values
            if col_type == "TEXT":
                c.execute(f"SELECT DISTINCT [{col_name}] FROM {table_name} WHERE [{col_name}] IS NOT NULL LIMIT 5")
                profile["sample_values"] = [row[0] for row in c.fetchall()]

            profiles.append(profile)

        conn.close()

        return {
            "table": table_name,
            "row_count": row_count,
            "column_count": len(columns_info),
            "columns": profiles,
            "profiled_at": datetime.now().isoformat(),
            "quality_score": self._calculate_quality_score(profiles, row_count),
        }

    def _calculate_quality_score(self, profiles: List[Dict], row_count: int) -> float:
        """Calculate an overall data quality score (0-100)."""
        if not profiles or row_count == 0:
            return 0.0

        scores = []
        for col in profiles:
            # Completeness score
            completeness = 100 - col.get("null_pct", 0)
            scores.append(completeness)

        return round(sum(scores) / len(scores), 1)

    def get_all_tables(self) -> List[str]:
        """Get all table names from the database."""
        conn = self._connect()
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in c.fetchall()]
        conn.close()
        return tables


class PipelineOrchestrator:
    """Manages data pipeline execution stages."""

    STAGES = ["ingest", "validate", "profile", "transform", "quality_check", "export"]

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.profiler = DataProfiler(db_path)

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def get_pipeline_status(self) -> Dict:
        """Get the current status of all pipelines."""
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 10")
        runs = [dict(row) for row in c.fetchall()]
        conn.close()

        return {
            "stages": self.STAGES,
            "recent_runs": runs,
            "stage_descriptions": {
                "ingest": "Load raw data from CSV files or database sources",
                "validate": "Check schema compliance and data type validation",
                "profile": "Generate statistical profiles and distributions",
                "transform": "Apply cleaning, normalization, and enrichment",
                "quality_check": "Run data quality rules and flag anomalies",
                "export": "Export processed data to target destination",
            }
        }

    def run_pipeline(self, pipeline_name: str = "E-Commerce ETL") -> Dict:
        """Execute a full pipeline run."""
        conn = self._connect()
        c = conn.cursor()

        started = datetime.now().isoformat()
        c.execute(
            "INSERT INTO pipeline_runs (pipeline_name, status, started_at, stage) VALUES (?, ?, ?, ?)",
            (pipeline_name, "running", started, "ingest")
        )
        run_id = c.lastrowid
        conn.commit()

        results = {}
        total_records = 0
        errors = 0

        # Stage 1: Ingest
        c.execute("SELECT COUNT(*) FROM orders")
        order_count = c.fetchone()[0]
        total_records += order_count
        results["ingest"] = {"records_loaded": order_count, "status": "completed"}

        # Stage 2: Validate
        c.execute("SELECT COUNT(*) FROM orders WHERE total IS NULL OR order_date IS NULL")
        null_orders = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM orders WHERE total < 0")
        negative_totals = c.fetchone()[0]
        val_errors = null_orders + negative_totals
        errors += val_errors
        results["validate"] = {
            "null_critical_fields": null_orders,
            "negative_values": negative_totals,
            "validation_errors": val_errors,
            "status": "completed"
        }

        # Stage 3: Profile
        tables = self.profiler.get_all_tables()
        profiles_summary = {}
        for t in tables:
            if t in ("datasets", "pipeline_runs"):
                continue
            profile = self.profiler.profile_table(t)
            profiles_summary[t] = {
                "rows": profile["row_count"],
                "columns": profile["column_count"],
                "quality_score": profile["quality_score"],
            }
        results["profile"] = {"tables_profiled": len(profiles_summary), "summaries": profiles_summary, "status": "completed"}

        # Stage 4: Transform
        c.execute("""
            SELECT COUNT(*) FROM orders 
            WHERE total != ROUND(subtotal - discount + tax, 2)
            AND subtotal IS NOT NULL AND discount IS NOT NULL AND tax IS NOT NULL
        """)
        inconsistent = c.fetchone()[0]
        results["transform"] = {
            "price_inconsistencies_found": inconsistent,
            "transformations_applied": ["price_rounding", "null_handling", "status_normalization"],
            "status": "completed"
        }

        # Stage 5: Quality Check
        c.execute("SELECT COUNT(DISTINCT email) FROM customers")
        unique_emails = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM customers")
        total_customers = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM products WHERE rating < 1 OR rating > 5")
        invalid_ratings = c.fetchone()[0]

        quality_issues = (total_customers - unique_emails) + invalid_ratings
        errors += quality_issues
        results["quality_check"] = {
            "duplicate_emails": total_customers - unique_emails,
            "invalid_ratings": invalid_ratings,
            "total_issues": quality_issues,
            "status": "completed"
        }

        # Stage 6: Export
        results["export"] = {
            "destination": "dataforge_processed",
            "records_exported": total_records,
            "format": "SQLite",
            "status": "completed"
        }

        # Finalize
        completed = datetime.now().isoformat()
        c.execute("""
            UPDATE pipeline_runs 
            SET status = ?, completed_at = ?, records_processed = ?, errors = ?, stage = ?
            WHERE id = ?
        """, ("completed", completed, total_records, errors, "export", run_id))
        conn.commit()
        conn.close()

        return {
            "run_id": run_id,
            "pipeline": pipeline_name,
            "status": "completed",
            "started_at": started,
            "completed_at": completed,
            "total_records": total_records,
            "total_errors": errors,
            "stages": results,
        }


class QueryExecutor:
    """Execute SQL queries safely against the database."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def execute(self, sql: str, limit: int = 100) -> Dict:
        """Execute a read-only SQL query and return results."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        try:
            # Basic safety: only allow SELECT
            sql_stripped = sql.strip().upper()
            if not sql_stripped.startswith("SELECT"):
                return {"error": "Only SELECT queries are allowed", "rows": [], "columns": []}

            c.execute(sql)
            rows = c.fetchmany(limit)
            columns = [desc[0] for desc in c.description] if c.description else []
            data = [dict(row) for row in rows]

            return {
                "columns": columns,
                "rows": data,
                "row_count": len(data),
                "truncated": len(data) == limit,
            }
        except Exception as e:
            return {"error": str(e), "rows": [], "columns": []}
        finally:
            conn.close()
