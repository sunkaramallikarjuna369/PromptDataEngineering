"""
DataForge AI — E-Commerce Data Engineering Platform
FastAPI Backend with Prompt Engineering Integration
"""

import os
import sqlite3
import json
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from sample_data import seed_data
from prompt_engine import engine as prompt_engine
from data_pipeline import DataProfiler, PipelineOrchestrator, QueryExecutor

# =============================================================================
# App Configuration
# =============================================================================

DB_PATH = "dataforge.db"
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

app = FastAPI(
    title="DataForge AI",
    description="E-Commerce Data Engineering Platform with Prompt Engineering",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# =============================================================================
# Pydantic Models
# =============================================================================

class PromptRequest(BaseModel):
    prompt: str

class SQLExecuteRequest(BaseModel):
    sql: str


# =============================================================================
# Startup
# =============================================================================

@app.on_event("startup")
async def startup():
    """Seed sample data if database doesn't exist."""
    if not os.path.exists(DB_PATH):
        seed_data(DB_PATH)
        print("✅ Database seeded with sample e-commerce data")
    else:
        print("📦 Existing database found")


# =============================================================================
# Root & Health
# =============================================================================

@app.get("/")
async def root():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat(), "app": "DataForge AI"}


# =============================================================================
# Dataset Endpoints
# =============================================================================

@app.get("/api/datasets")
async def list_datasets():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM datasets ORDER BY id DESC")
    datasets = [dict(row) for row in c.fetchall()]
    conn.close()
    return {"datasets": datasets}


@app.post("/api/datasets/seed")
async def seed_database():
    result = seed_data(DB_PATH)
    return result


@app.post("/api/datasets/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    content = await file.read()
    lines = content.decode("utf-8").strip().split("\n")
    row_count = len(lines) - 1  # Exclude header
    col_count = len(lines[0].split(","))

    # Record the upload
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO datasets (name, source, row_count, column_count, created_at, status) VALUES (?, ?, ?, ?, ?, ?)",
        (file.filename, "upload", row_count, col_count, datetime.now().isoformat(), "active")
    )
    conn.commit()
    conn.close()

    return {
        "message": f"Uploaded {file.filename}",
        "rows": row_count,
        "columns": col_count,
    }


# =============================================================================
# Data Profiling Endpoints
# =============================================================================

@app.get("/api/datasets/{dataset_id}/profile")
async def get_profile(dataset_id: int):
    profiler = DataProfiler(DB_PATH)
    tables = profiler.get_all_tables()
    # Exclude meta tables
    data_tables = [t for t in tables if t not in ("datasets", "pipeline_runs")]

    profiles = {}
    for table in data_tables:
        profiles[table] = profiler.profile_table(table)

    return {"dataset_id": dataset_id, "profiles": profiles}


@app.get("/api/profile/{table_name}")
async def profile_table(table_name: str):
    profiler = DataProfiler(DB_PATH)
    valid_tables = profiler.get_all_tables()
    if table_name not in valid_tables:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    return profiler.profile_table(table_name)


# =============================================================================
# Prompt Engineering Endpoints
# =============================================================================

@app.post("/api/prompt/sql")
async def prompt_to_sql(request: PromptRequest):
    result = prompt_engine.generate_sql(request.prompt)

    # Execute the generated SQL
    executor = QueryExecutor(DB_PATH)
    query_result = executor.execute(result["sql"])

    return {
        "prompt": request.prompt,
        "generated": result,
        "result": query_result,
    }


@app.post("/api/prompt/transform")
async def prompt_to_transform(request: PromptRequest):
    result = prompt_engine.generate_transform(request.prompt)
    return {
        "prompt": request.prompt,
        "generated": result,
    }


@app.post("/api/prompt/quality")
async def prompt_to_quality(request: PromptRequest):
    result = prompt_engine.generate_quality_rules(request.prompt)
    return {
        "prompt": request.prompt,
        "generated": result,
    }


@app.post("/api/query/execute")
async def execute_query(request: SQLExecuteRequest):
    executor = QueryExecutor(DB_PATH)
    return executor.execute(request.sql)


# =============================================================================
# Pipeline Endpoints
# =============================================================================

@app.get("/api/pipeline/status")
async def pipeline_status():
    orchestrator = PipelineOrchestrator(DB_PATH)
    return orchestrator.get_pipeline_status()


@app.post("/api/pipeline/run")
async def run_pipeline():
    orchestrator = PipelineOrchestrator(DB_PATH)
    return orchestrator.run_pipeline()


# =============================================================================
# Dashboard KPI Endpoints
# =============================================================================

@app.get("/api/dashboard/kpis")
async def dashboard_kpis():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    kpis = {}

    # Total Revenue
    c.execute("SELECT ROUND(SUM(total), 2) FROM orders WHERE status != 'Cancelled'")
    kpis["total_revenue"] = c.fetchone()[0] or 0

    # Total Orders
    c.execute("SELECT COUNT(*) FROM orders WHERE status != 'Cancelled'")
    kpis["total_orders"] = c.fetchone()[0]

    # Total Customers
    c.execute("SELECT COUNT(*) FROM customers")
    kpis["total_customers"] = c.fetchone()[0]

    # Average Order Value
    c.execute("SELECT ROUND(AVG(total), 2) FROM orders WHERE status != 'Cancelled'")
    kpis["avg_order_value"] = c.fetchone()[0] or 0

    # Total Products
    c.execute("SELECT COUNT(*) FROM products")
    kpis["total_products"] = c.fetchone()[0]

    # Revenue by Category
    c.execute("""
        SELECT c.name, ROUND(SUM(oi.line_total), 2) AS revenue
        FROM categories c
        JOIN products p ON p.category_id = c.id
        JOIN order_items oi ON oi.product_id = p.id
        JOIN orders o ON o.id = oi.order_id
        WHERE o.status != 'Cancelled'
        GROUP BY c.name
        ORDER BY revenue DESC
    """)
    kpis["revenue_by_category"] = [{"name": row[0], "revenue": row[1]} for row in c.fetchall()]

    # Revenue by Region
    c.execute("""
        SELECT region, ROUND(SUM(total), 2) AS revenue, COUNT(*) AS orders
        FROM orders
        WHERE status != 'Cancelled'
        GROUP BY region
        ORDER BY revenue DESC
    """)
    kpis["revenue_by_region"] = [{"name": row[0], "revenue": row[1], "orders": row[2]} for row in c.fetchall()]

    # Monthly Trend
    c.execute("""
        SELECT strftime('%Y-%m', order_date) AS month,
               ROUND(SUM(total), 2) AS revenue,
               COUNT(*) AS orders
        FROM orders
        WHERE status != 'Cancelled'
        GROUP BY month
        ORDER BY month
    """)
    kpis["monthly_trend"] = [{"month": row[0], "revenue": row[1], "orders": row[2]} for row in c.fetchall()]

    # Order Status Breakdown
    c.execute("""
        SELECT status, COUNT(*) AS count
        FROM orders GROUP BY status ORDER BY count DESC
    """)
    kpis["order_status"] = [{"status": row[0], "count": row[1]} for row in c.fetchall()]

    # Top 5 Products
    c.execute("""
        SELECT p.name, ROUND(SUM(oi.line_total), 2) AS revenue
        FROM products p
        JOIN order_items oi ON oi.product_id = p.id
        JOIN orders o ON o.id = oi.order_id
        WHERE o.status != 'Cancelled'
        GROUP BY p.id
        ORDER BY revenue DESC
        LIMIT 5
    """)
    kpis["top_products"] = [{"name": row[0], "revenue": row[1]} for row in c.fetchall()]

    # Payment Methods
    c.execute("""
        SELECT payment_method, COUNT(*) as count, ROUND(SUM(total), 2) as total
        FROM orders WHERE status != 'Cancelled'
        GROUP BY payment_method ORDER BY count DESC
    """)
    kpis["payment_methods"] = [{"method": row[0], "count": row[1], "total": row[2]} for row in c.fetchall()]

    conn.close()
    return kpis


# =============================================================================
# Tables Endpoint
# =============================================================================

@app.get("/api/tables")
async def list_tables():
    profiler = DataProfiler(DB_PATH)
    tables = profiler.get_all_tables()
    result = []
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for t in tables:
        c.execute(f"SELECT COUNT(*) FROM {t}")
        count = c.fetchone()[0]
        c.execute(f"PRAGMA table_info({t})")
        cols = len(c.fetchall())
        result.append({"name": t, "rows": count, "columns": cols})
    conn.close()
    return {"tables": result}
