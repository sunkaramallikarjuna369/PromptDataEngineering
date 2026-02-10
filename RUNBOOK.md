# DataForge AI — Execution Runbook

## Prerequisites
- **Python 3.8+** installed
- **pip** package manager

## Quick Start

### 1. Install Dependencies
```bash
cd d:\cluebot_whatsapp
pip install -r requirements.txt
```

### 2. Start the Server
```bash
cd d:\cluebot_whatsapp
python -m uvicorn main:app --reload --port 8000
```

### 3. Open the Application
Open your browser and navigate to:  
**http://localhost:8000**

---

## Application Pages

| Page | URL Path | Description |
|------|----------|-------------|
| Dashboard | `/` | KPI cards, revenue charts, category & region breakdowns |
| Prompt Studio | Sidebar → 🧠 | Natural language → SQL / Transformations / Quality Rules |
| Pipeline | Sidebar → 🔄 | Visual ETL pipeline with 6 stages, run & history |
| Data Profiler | Sidebar → 🔍 | Column-level statistics for any table |
| Quality Monitor | Sidebar → ✅ | AI-suggested data quality rules |
| Data Explorer | Sidebar → 📁 | Raw SQL query executor with table browser |

## Prompt Studio — Example Queries

### Prompt → SQL
- `Show top 10 products by revenue`
- `Revenue by category`
- `Monthly revenue trend`
- `Top 5 customers by spending`
- `Profit analysis`
- `Payment method distribution`

### Prompt → Transform
- `Clean null values in dataset`
- `RFM customer segmentation`
- `Normalize price fields`

### Prompt → Quality Rules
- `Check for missing values and duplicates`
- `Full quality assessment`

## API Endpoints (for testing)

```bash
# Health check
curl http://localhost:8000/api/health

# Dashboard KPIs
curl http://localhost:8000/api/dashboard/kpis

# Re-seed sample data
curl -X POST http://localhost:8000/api/datasets/seed

# Prompt → SQL
curl -X POST http://localhost:8000/api/prompt/sql -H "Content-Type: application/json" -d "{\"prompt\": \"show top 10 products by revenue\"}"

# Run pipeline
curl -X POST http://localhost:8000/api/pipeline/run

# Execute raw SQL
curl -X POST http://localhost:8000/api/query/execute -H "Content-Type: application/json" -d "{\"sql\": \"SELECT * FROM orders LIMIT 5\"}"
```

## Stopping the Server
Press `Ctrl + C` in the terminal where the server is running.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Port 8000 in use | Use `--port 8001` flag instead |
| Module not found | Run `pip install -r requirements.txt` again |
| Database issues | Delete `dataforge.db` — it auto-regenerates on startup |
