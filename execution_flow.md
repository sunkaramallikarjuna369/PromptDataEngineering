# DataForge AI — Execution Flow

This document describes the step-by-step execution flow of the DataForge AI platform, from startup to each feature interaction.

---

## 1. Application Startup

```
pip install -r requirements.txt
python -m uvicorn main:app --reload --port 8000
```

**What happens on startup:**
1. FastAPI server starts on `http://localhost:8000`
2. The app checks if `dataforge.db` exists
3. If not, `seed_data()` runs automatically — creating tables and populating:
   - 8 product categories
   - 34 products with prices, costs, stock, and ratings
   - 200 customers across 5 global regions
   - 500 orders with 1,117 order items
4. Static files are mounted at `/static`
5. CORS middleware is enabled for cross-origin access

---

## 2. Frontend Loading

When a user navigates to `http://localhost:8000`:
1. `GET /` serves `static/index.html`
2. The single-page app loads with a sidebar and 6 page sections
3. JavaScript calls `GET /api/dashboard/kpis` to populate the Dashboard
4. Chart.js renders 4 interactive charts from the KPI data

---

## 3. Dashboard Flow

![Dashboard](screenshots/01_dashboard.png)

**API Call:** `GET /api/dashboard/kpis`

**Execution Steps:**
1. Connects to SQLite database
2. Runs 8 aggregate queries:
   - Total revenue (excluding cancelled orders)
   - Total order count
   - Total customer count
   - Average order value
   - Revenue by category (JOIN categories > products > order_items > orders)
   - Revenue by region
   - Monthly revenue trend
   - Order status breakdown
3. Returns JSON with all KPI data
4. Frontend renders KPI cards and Chart.js charts

---

## 4. Prompt Studio Flow

### 4a. Prompt to SQL

![Prompt to SQL](screenshots/02_prompt_studio_sql.png)

**API Call:** `POST /api/prompt/sql` with `{"prompt": "..."}`

**Execution Steps:**
1. **Intent Classification** — Regex patterns match the prompt against 15 intent categories (revenue, product, customer, trend, etc.)
2. **Entity Extraction** — Extracts numbers, dates, aggregation types, and sort orders
3. **Template Selection** — Maps intent combination to one of 13 SQL templates
4. **Few-Shot Matching** — Finds the most relevant example from 5 curated few-shot examples
5. **Chain-of-Thought** — Builds a reasoning trace showing classification, entities, template, and limit
6. **SQL Execution** — The generated SQL is executed via `QueryExecutor` (read-only, SELECT only)
7. **Response** — Returns generated SQL, reasoning, query results, and the prompt technique used

### 4b. Prompt to Transform

![Prompt to Transform](screenshots/03_prompt_studio_transform.png)

**API Call:** `POST /api/prompt/transform` with `{"prompt": "..."}`

**Execution Steps:**
1. **Keyword Matching** — Scans prompt against keyword lists for 6 transformation templates
2. **Scoring** — Each template gets a relevance score based on keyword hits
3. **Selection** — Highest-scoring template is selected
4. **Response** — Returns transformation name, description, step-by-step plan, and generated Python code

**Available Transformations:**
- Clean Null Values
- Normalize Price Fields
- Aggregate Revenue Metrics
- RFM Customer Segmentation
- Deduplicate Records
- Date Feature Engineering

### 4c. Prompt to Quality Rules

![Prompt to Quality Rules](screenshots/04_prompt_studio_quality.png)

**API Call:** `POST /api/prompt/quality` with `{"prompt": "..."}`

**Execution Steps:**
1. **Category Matching** — Maps prompt keywords to 5 quality rule categories
2. **Rule Selection** — Pulls all rules from matched categories
3. **Response** — Returns rules grouped by category with severity levels (critical/high/medium/low)

---

## 5. Pipeline Flow

![Pipeline Orchestrator](screenshots/05_pipeline.png)

**API Calls:**
- `GET /api/pipeline/status` — Load pipeline state and history
- `POST /api/pipeline/run` — Execute full pipeline

**Execution Steps (6 stages):**

| Stage | Action | Key Metrics |
|-------|--------|-------------|
| **Ingest** | Count records from orders table | `records_loaded` |
| **Validate** | Check for NULL critical fields and negative values | `validation_errors` |
| **Profile** | Run `DataProfiler` on all data tables | `tables_profiled`, quality scores |
| **Transform** | Check price inconsistencies (subtotal - discount + tax = total) | `price_inconsistencies_found` |
| **Quality Check** | Verify email uniqueness and rating validity | `duplicate_emails`, `invalid_ratings` |
| **Export** | Record processed data summary | `records_exported`, format |

Each run is logged to the `pipeline_runs` table with timestamps, record counts, and error counts.

---

## 6. Data Profiler Flow

![Data Profiler](screenshots/06_data_profiler.png)

**API Calls:**
- `GET /api/tables` — List all tables with row/column counts
- `GET /api/profile/{table_name}` — Profile a specific table

**Execution Steps:**
1. User selects a table from the dropdown
2. `DataProfiler.profile_table()` runs:
   - Gets column metadata via `PRAGMA table_info`
   - For each column: counts nulls, distinct values
   - For numeric columns: computes min, max, mean, sum
   - For text columns: retrieves 5 sample values
3. Calculates overall quality score (based on completeness)
4. Frontend renders profile cards with statistics and quality bars

---

## 7. Quality Monitor Flow

![Quality Monitor](screenshots/07_quality_monitor.png)

**API Call:** `POST /api/prompt/quality` with `{"prompt": "..."}`

**Execution Steps:**
1. User enters a quality concern (e.g., "Check for missing values and duplicates")
2. `PromptEngine.generate_quality_rules()` matches against 5 categories:
   - **Completeness** — Required field validation (5 rules)
   - **Consistency** — Logical relationship checks (4 rules)
   - **Validity** — Range and boundary enforcement (4 rules)
   - **Uniqueness** — Duplicate detection (3 rules)
   - **Timeliness** — Data freshness verification (2 rules)
3. Returns matched rules with severity badges

---

## 8. Data Explorer Flow

![Data Explorer](screenshots/08_data_explorer.png)

**API Calls:**
- `GET /api/tables` — Populate table browser
- `POST /api/query/execute` with `{"sql": "..."}`

**Execution Steps:**
1. Table browser loads automatically showing all tables with row/column counts
2. User writes a SQL query in the input field
3. `QueryExecutor.execute()` runs the query:
   - Validates that only `SELECT` statements are allowed
   - Executes against SQLite database
   - Returns up to 100 rows with column names
4. Frontend renders results in a data table

---

## Request/Response Architecture

```
Browser (SPA)
    |
    |  HTTP / JSON
    v
FastAPI (main.py)
    |
    |--- PromptEngine (prompt_engine.py)
    |       |--- Intent Classification
    |       |--- Entity Extraction
    |       |--- Template Matching
    |       |--- SQL / Transform / Quality Generation
    |
    |--- DataProfiler (data_pipeline.py)
    |       |--- Table Profiling
    |       |--- Quality Scoring
    |
    |--- PipelineOrchestrator (data_pipeline.py)
    |       |--- 6-Stage ETL Execution
    |       |--- Run History Tracking
    |
    |--- QueryExecutor (data_pipeline.py)
    |       |--- Read-Only SQL Execution
    |
    v
SQLite (dataforge.db)
    |--- categories, products, customers
    |--- orders, order_items
    |--- datasets, pipeline_runs
```
