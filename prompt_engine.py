"""
Prompt Engineering Module for DataForge AI
Demonstrates key prompt engineering techniques:
  - Few-shot prompting with examples
  - Chain-of-thought reasoning
  - Role prompting (system/user/assistant patterns)
  - Template-based structured output generation
  - Intent classification and entity extraction
"""

import re
import json
from typing import Dict, List, Optional


# =============================================================================
# PROMPT TEMPLATES — Core prompt engineering patterns
# =============================================================================

SQL_SYSTEM_PROMPT = """You are a senior SQL analyst working with an e-commerce database.
The database has these tables:

TABLES:
- categories (id, name, margin)
- products (id, name, category_id, price, cost, stock, rating, reviews_count, created_at)
- customers (id, first_name, last_name, email, region, city, signup_date, lifetime_value, order_count)
- orders (id, customer_id, order_date, status, payment_method, subtotal, discount, tax, total, region)
- order_items (id, order_id, product_id, quantity, unit_price, line_total)

RULES:
1. Always use proper JOIN syntax
2. Use aliases for readability
3. Add ORDER BY for sorted results
4. Use LIMIT for top-N queries
5. Format numbers with ROUND() where appropriate
"""

# Few-shot examples for SQL generation
SQL_FEW_SHOT_EXAMPLES = [
    {
        "input": "show total revenue by category",
        "reasoning": "Need to join orders → order_items → products → categories, then SUM the line_total grouped by category",
        "output": """SELECT c.name AS category, 
       ROUND(SUM(oi.line_total), 2) AS total_revenue,
       COUNT(DISTINCT o.id) AS total_orders
FROM categories c
JOIN products p ON p.category_id = c.id
JOIN order_items oi ON oi.product_id = p.id
JOIN orders o ON o.id = oi.order_id
WHERE o.status != 'Cancelled'
GROUP BY c.name
ORDER BY total_revenue DESC"""
    },
    {
        "input": "find top 5 customers by spending",
        "reasoning": "Need customer names and sum of their order totals, excluding cancelled orders, limited to 5",
        "output": """SELECT c.first_name || ' ' || c.last_name AS customer_name,
       c.email,
       ROUND(SUM(o.total), 2) AS total_spent,
       COUNT(o.id) AS order_count
FROM customers c
JOIN orders o ON o.customer_id = c.id
WHERE o.status != 'Cancelled'
GROUP BY c.id
ORDER BY total_spent DESC
LIMIT 5"""
    },
    {
        "input": "show monthly revenue trend",
        "reasoning": "Extract year-month from order_date, sum totals, order chronologically",
        "output": """SELECT strftime('%Y-%m', o.order_date) AS month,
       ROUND(SUM(o.total), 2) AS revenue,
       COUNT(o.id) AS orders,
       ROUND(AVG(o.total), 2) AS avg_order_value
FROM orders o
WHERE o.status != 'Cancelled'
GROUP BY month
ORDER BY month"""
    },
    {
        "input": "what are the best rated products",
        "reasoning": "Select from products, order by rating descending, include review count for context",
        "output": """SELECT p.name AS product,
       c.name AS category,
       p.rating,
       p.reviews_count,
       p.price
FROM products p
JOIN categories c ON c.id = p.category_id
WHERE p.reviews_count >= 50
ORDER BY p.rating DESC, p.reviews_count DESC
LIMIT 10"""
    },
    {
        "input": "show revenue by region",
        "reasoning": "Group orders by region, sum totals, count distinct customers",
        "output": """SELECT o.region,
       ROUND(SUM(o.total), 2) AS total_revenue,
       COUNT(o.id) AS total_orders,
       COUNT(DISTINCT o.customer_id) AS unique_customers,
       ROUND(AVG(o.total), 2) AS avg_order_value
FROM orders o
WHERE o.status != 'Cancelled'
GROUP BY o.region
ORDER BY total_revenue DESC"""
    },
]

# Intent patterns for query classification
INTENT_PATTERNS = {
    "revenue": r"(?i)(revenue|sales|income|earning|money|amount)",
    "top_products": r"(?i)(top|best|popular|highest|most sold)\s*(product|item)",
    "top_customers": r"(?i)(top|best|biggest|highest|vip)\s*(customer|buyer|client|spender)",
    "orders": r"(?i)(order|purchase|transaction|buy)",
    "trend": r"(?i)(trend|monthly|weekly|daily|over time|growth|timeline)",
    "category": r"(?i)(category|categor|department|section|type)",
    "region": r"(?i)(region|country|location|city|geography|area)",
    "inventory": r"(?i)(stock|inventory|supply|available|out of stock)",
    "rating": r"(?i)(rating|review|rated|star|quality|score)",
    "discount": r"(?i)(discount|coupon|promo|offer|deal|savings)",
    "customer": r"(?i)(customer|client|user|buyer|subscriber)",
    "product": r"(?i)(product|item|goods|merchandise)",
    "payment": r"(?i)(payment|pay|method|credit|paypal|debit)",
    "refund": r"(?i)(refund|return|cancel|chargeback)",
    "profit": r"(?i)(profit|margin|markup|cost|expense)",
}

# Entity extraction patterns
ENTITY_PATTERNS = {
    "number": r"(\d+)",
    "date": r"(\d{4}-\d{2}-\d{2})",
    "month": r"(?i)(january|february|march|april|may|june|july|august|september|october|november|december)",
    "year": r"(20\d{2})",
    "comparison": r"(?i)(greater than|less than|more than|fewer than|above|below|over|under|between)",
    "aggregation": r"(?i)(total|sum|average|avg|count|max|min|mean|median)",
    "sort_order": r"(?i)(ascending|descending|asc|desc|highest|lowest|most|least)",
}


# =============================================================================
# SQL QUERY TEMPLATES — Structured outputs mapped to intents
# =============================================================================

SQL_TEMPLATES = {
    "revenue_by_category": """SELECT c.name AS category, 
       ROUND(SUM(oi.line_total), 2) AS total_revenue,
       COUNT(DISTINCT o.id) AS total_orders
FROM categories c
JOIN products p ON p.category_id = c.id
JOIN order_items oi ON oi.product_id = p.id
JOIN orders o ON o.id = oi.order_id
WHERE o.status != 'Cancelled'
GROUP BY c.name
ORDER BY total_revenue DESC""",

    "revenue_by_region": """SELECT o.region,
       ROUND(SUM(o.total), 2) AS total_revenue,
       COUNT(o.id) AS total_orders,
       COUNT(DISTINCT o.customer_id) AS unique_customers,
       ROUND(AVG(o.total), 2) AS avg_order_value
FROM orders o
WHERE o.status != 'Cancelled'
GROUP BY o.region
ORDER BY total_revenue DESC""",

    "revenue_trend": """SELECT strftime('%Y-%m', o.order_date) AS month,
       ROUND(SUM(o.total), 2) AS revenue,
       COUNT(o.id) AS orders,
       ROUND(AVG(o.total), 2) AS avg_order_value
FROM orders o
WHERE o.status != 'Cancelled'
GROUP BY month
ORDER BY month""",

    "top_products_revenue": """SELECT p.name AS product,
       c.name AS category,
       ROUND(SUM(oi.line_total), 2) AS revenue,
       SUM(oi.quantity) AS units_sold
FROM products p
JOIN categories c ON c.id = p.category_id
JOIN order_items oi ON oi.product_id = p.id
JOIN orders o ON o.id = oi.order_id
WHERE o.status != 'Cancelled'
GROUP BY p.id
ORDER BY revenue DESC
LIMIT {limit}""",

    "top_products_rating": """SELECT p.name AS product,
       c.name AS category,
       p.rating,
       p.reviews_count,
       p.price
FROM products p
JOIN categories c ON c.id = p.category_id
WHERE p.reviews_count >= 10
ORDER BY p.rating DESC, p.reviews_count DESC
LIMIT {limit}""",

    "top_customers": """SELECT c.first_name || ' ' || c.last_name AS customer_name,
       c.email,
       c.region,
       c.city,
       ROUND(c.lifetime_value, 2) AS total_spent,
       c.order_count
FROM customers c
WHERE c.order_count > 0
ORDER BY c.lifetime_value DESC
LIMIT {limit}""",

    "order_status_breakdown": """SELECT o.status,
       COUNT(o.id) AS order_count,
       ROUND(SUM(o.total), 2) AS total_value,
       ROUND(AVG(o.total), 2) AS avg_value
FROM orders o
GROUP BY o.status
ORDER BY order_count DESC""",

    "payment_analysis": """SELECT o.payment_method,
       COUNT(o.id) AS usage_count,
       ROUND(SUM(o.total), 2) AS total_processed,
       ROUND(AVG(o.total), 2) AS avg_transaction
FROM orders o
WHERE o.status != 'Cancelled'
GROUP BY o.payment_method
ORDER BY usage_count DESC""",

    "inventory_status": """SELECT p.name AS product,
       c.name AS category,
       p.stock,
       p.price,
       CASE
           WHEN p.stock = 0 THEN 'Out of Stock'
           WHEN p.stock < 20 THEN 'Low Stock'
           WHEN p.stock < 100 THEN 'Normal'
           ELSE 'Well Stocked'
       END AS stock_status
FROM products p
JOIN categories c ON c.id = p.category_id
ORDER BY p.stock ASC""",

    "profit_analysis": """SELECT p.name AS product,
       c.name AS category,
       p.price,
       p.cost,
       ROUND(p.price - p.cost, 2) AS profit_per_unit,
       ROUND(((p.price - p.cost) / p.price) * 100, 1) AS margin_pct,
       SUM(oi.quantity) AS units_sold,
       ROUND(SUM(oi.quantity * (p.price - p.cost)), 2) AS total_profit
FROM products p
JOIN categories c ON c.id = p.category_id
JOIN order_items oi ON oi.product_id = p.id
JOIN orders o ON o.id = oi.order_id
WHERE o.status != 'Cancelled'
GROUP BY p.id
ORDER BY total_profit DESC
LIMIT {limit}""",

    "refund_analysis": """SELECT strftime('%Y-%m', o.order_date) AS month,
       COUNT(CASE WHEN o.status = 'Refunded' THEN 1 END) AS refunds,
       COUNT(CASE WHEN o.status = 'Cancelled' THEN 1 END) AS cancellations,
       COUNT(o.id) AS total_orders,
       ROUND(COUNT(CASE WHEN o.status IN ('Refunded', 'Cancelled') THEN 1 END) * 100.0 / COUNT(o.id), 1) AS issue_rate_pct
FROM orders o
GROUP BY month
ORDER BY month""",

    "customer_segments": """SELECT 
       CASE
           WHEN c.lifetime_value >= 1000 THEN 'VIP ($1000+)'
           WHEN c.lifetime_value >= 500 THEN 'High Value ($500-999)'
           WHEN c.lifetime_value >= 200 THEN 'Medium ($200-499)'
           WHEN c.lifetime_value > 0 THEN 'Low (<$200)'
           ELSE 'Inactive'
       END AS segment,
       COUNT(c.id) AS customer_count,
       ROUND(AVG(c.lifetime_value), 2) AS avg_ltv,
       ROUND(SUM(c.lifetime_value), 2) AS total_ltv
FROM customers c
GROUP BY segment
ORDER BY avg_ltv DESC""",

    "general_stats": """SELECT 
       (SELECT COUNT(*) FROM orders WHERE status != 'Cancelled') AS total_orders,
       (SELECT ROUND(SUM(total), 2) FROM orders WHERE status != 'Cancelled') AS total_revenue,
       (SELECT COUNT(*) FROM customers) AS total_customers,
       (SELECT COUNT(*) FROM products) AS total_products,
       (SELECT ROUND(AVG(total), 2) FROM orders WHERE status != 'Cancelled') AS avg_order_value"""
}


# =============================================================================
# TRANSFORMATION TEMPLATES
# =============================================================================

TRANSFORM_TEMPLATES = {
    "clean_nulls": {
        "name": "Clean Null Values",
        "description": "Remove or fill null/missing values in the dataset",
        "steps": [
            "Identify columns with null values",
            "For numeric columns: fill with median value",
            "For categorical columns: fill with mode value",
            "Drop rows where critical fields (id, date) are null",
            "Log the count of nulls cleaned per column"
        ],
        "code": """import pandas as pd

def clean_nulls(df):
    report = {}
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            report[col] = null_count
            if df[col].dtype in ['float64', 'int64']:
                df[col].fillna(df[col].median(), inplace=True)
            else:
                df[col].fillna(df[col].mode()[0], inplace=True)
    return df, report"""
    },
    "normalize_prices": {
        "name": "Normalize Price Fields",
        "description": "Standardize price columns to 2 decimal places and handle outliers",
        "steps": [
            "Round all price fields to 2 decimal places",
            "Identify outliers using IQR method",
            "Flag outlier rows for review",
            "Ensure no negative prices exist"
        ],
        "code": """import pandas as pd
import numpy as np

def normalize_prices(df, price_cols):
    for col in price_cols:
        df[col] = df[col].round(2)
        df[col] = df[col].clip(lower=0)
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        df[f'{col}_outlier'] = (df[col] < Q1 - 1.5*IQR) | (df[col] > Q3 + 1.5*IQR)
    return df"""
    },
    "aggregate_revenue": {
        "name": "Aggregate Revenue Metrics",
        "description": "Create aggregated revenue views by different dimensions",
        "steps": [
            "Group by time period (daily/monthly/quarterly)",
            "Calculate sum, average, and count metrics",
            "Add running total and moving averages",
            "Calculate period-over-period growth rates"
        ],
        "code": """import pandas as pd

def aggregate_revenue(df, period='M'):
    df['order_date'] = pd.to_datetime(df['order_date'])
    agg = df.groupby(df['order_date'].dt.to_period(period)).agg(
        revenue=('total', 'sum'),
        orders=('id', 'count'),
        avg_order=('total', 'mean')
    ).reset_index()
    agg['running_total'] = agg['revenue'].cumsum()
    agg['growth_pct'] = agg['revenue'].pct_change() * 100
    return agg"""
    },
    "customer_rfm": {
        "name": "RFM Customer Segmentation",
        "description": "Compute Recency, Frequency, Monetary scores for customer segmentation",
        "steps": [
            "Calculate days since last purchase (Recency)",
            "Count total orders per customer (Frequency)",
            "Sum total spend per customer (Monetary)",
            "Score each dimension on 1-5 scale",
            "Assign customer segments based on combined RFM score"
        ],
        "code": """import pandas as pd
from datetime import datetime

def rfm_segmentation(orders_df, reference_date=None):
    if reference_date is None:
        reference_date = datetime.now()
    
    rfm = orders_df.groupby('customer_id').agg(
        recency=('order_date', lambda x: (reference_date - pd.to_datetime(x).max()).days),
        frequency=('id', 'count'),
        monetary=('total', 'sum')
    ).reset_index()
    
    for col in ['recency', 'frequency', 'monetary']:
        ascending = col == 'recency'
        rfm[f'{col}_score'] = pd.qcut(rfm[col], q=5, labels=[5,4,3,2,1] if ascending else [1,2,3,4,5])
    
    rfm['rfm_score'] = rfm['recency_score'].astype(int) + rfm['frequency_score'].astype(int) + rfm['monetary_score'].astype(int)
    return rfm"""
    },
    "deduplicate": {
        "name": "Deduplicate Records",
        "description": "Identify and remove duplicate records from the dataset",
        "steps": [
            "Identify exact duplicates across all columns",
            "Identify near-duplicates using key columns",
            "Keep first occurrence, flag duplicates",
            "Log deduplication summary"
        ],
        "code": """import pandas as pd

def deduplicate(df, key_cols=None):
    before = len(df)
    if key_cols:
        df = df.drop_duplicates(subset=key_cols, keep='first')
    else:
        df = df.drop_duplicates(keep='first')
    after = len(df)
    return df, {'removed': before - after, 'remaining': after}"""
    },
    "enrich_dates": {
        "name": "Date Feature Engineering",
        "description": "Extract rich temporal features from date columns",
        "steps": [
            "Parse date strings into datetime objects",
            "Extract year, month, day, day_of_week, quarter",
            "Add is_weekend and is_holiday flags",
            "Calculate days_since_epoch for ML features"
        ],
        "code": """import pandas as pd

def enrich_dates(df, date_col='order_date'):
    df[date_col] = pd.to_datetime(df[date_col])
    df['year'] = df[date_col].dt.year
    df['month'] = df[date_col].dt.month
    df['day'] = df[date_col].dt.day
    df['day_of_week'] = df[date_col].dt.day_name()
    df['quarter'] = df[date_col].dt.quarter
    df['is_weekend'] = df[date_col].dt.weekday >= 5
    return df"""
    },
}


# =============================================================================
# DATA QUALITY RULES
# =============================================================================

QUALITY_RULES = {
    "completeness": {
        "name": "Completeness Check",
        "description": "Verify all required fields are populated",
        "rules": [
            {"field": "order_date", "rule": "NOT NULL", "severity": "critical"},
            {"field": "customer_id", "rule": "NOT NULL", "severity": "critical"},
            {"field": "total", "rule": "NOT NULL AND >= 0", "severity": "critical"},
            {"field": "email", "rule": "NOT NULL AND VALID FORMAT", "severity": "high"},
            {"field": "region", "rule": "NOT NULL", "severity": "medium"},
        ]
    },
    "consistency": {
        "name": "Consistency Check",
        "description": "Ensure data values are logically consistent",
        "rules": [
            {"field": "total", "rule": "total ≈ subtotal - discount + tax", "severity": "critical"},
            {"field": "line_total", "rule": "line_total = quantity × unit_price", "severity": "critical"},
            {"field": "price", "rule": "price > cost (positive margin)", "severity": "high"},
            {"field": "order_date", "rule": "order_date >= customer.signup_date", "severity": "medium"},
        ]
    },
    "validity": {
        "name": "Validity Check",
        "description": "Ensure values fall within acceptable ranges",
        "rules": [
            {"field": "rating", "rule": "1.0 <= rating <= 5.0", "severity": "high"},
            {"field": "quantity", "rule": "quantity > 0 AND quantity <= 100", "severity": "high"},
            {"field": "discount", "rule": "discount >= 0 AND discount <= subtotal", "severity": "medium"},
            {"field": "stock", "rule": "stock >= 0", "severity": "low"},
        ]
    },
    "uniqueness": {
        "name": "Uniqueness Check",
        "description": "Verify unique constraints are maintained",
        "rules": [
            {"field": "customer.email", "rule": "UNIQUE across all customers", "severity": "critical"},
            {"field": "order.id", "rule": "UNIQUE primary key", "severity": "critical"},
            {"field": "product.name", "rule": "UNIQUE within category", "severity": "medium"},
        ]
    },
    "timeliness": {
        "name": "Timeliness Check",
        "description": "Verify data freshness and temporal validity",
        "rules": [
            {"field": "order_date", "rule": "Within expected date range (2024-2025)", "severity": "medium"},
            {"field": "created_at", "rule": "Not in the future", "severity": "high"},
        ]
    }
}


# =============================================================================
# PROMPT ENGINE — Core logic
# =============================================================================

class PromptEngine:
    """
    Template-based prompt engineering engine for data engineering tasks.
    Demonstrates production-grade prompt patterns without requiring external LLM APIs.
    """

    def __init__(self):
        self.sql_templates = SQL_TEMPLATES
        self.transform_templates = TRANSFORM_TEMPLATES
        self.quality_rules = QUALITY_RULES
        self.intent_patterns = INTENT_PATTERNS
        self.entity_patterns = ENTITY_PATTERNS

    def classify_intent(self, prompt: str) -> List[str]:
        """Classify the user's intent from their natural language prompt."""
        intents = []
        for intent, pattern in self.intent_patterns.items():
            if re.search(pattern, prompt):
                intents.append(intent)
        return intents if intents else ["general"]

    def extract_entities(self, prompt: str) -> Dict:
        """Extract structured entities from the prompt."""
        entities = {}
        for entity_type, pattern in self.entity_patterns.items():
            matches = re.findall(pattern, prompt)
            if matches:
                entities[entity_type] = matches
        return entities

    def generate_sql(self, prompt: str) -> Dict:
        """
        Generate SQL from natural language using prompt engineering techniques:
        1. Intent classification
        2. Entity extraction
        3. Template matching with few-shot examples
        4. Chain-of-thought reasoning trace
        """
        intents = self.classify_intent(prompt)
        entities = self.extract_entities(prompt)
        limit = 10
        if "number" in entities:
            limit = int(entities["number"][0])

        # Chain-of-thought reasoning
        reasoning_steps = [
            f"1. Classified intents: {intents}",
            f"2. Extracted entities: {entities}",
        ]

        # Template selection based on intent combination
        sql = ""
        template_used = ""

        if "trend" in intents and "revenue" in intents:
            sql = self.sql_templates["revenue_trend"]
            template_used = "revenue_trend"
        elif "revenue" in intents and "category" in intents:
            sql = self.sql_templates["revenue_by_category"]
            template_used = "revenue_by_category"
        elif "revenue" in intents and "region" in intents:
            sql = self.sql_templates["revenue_by_region"]
            template_used = "revenue_by_region"
        elif "profit" in intents:
            sql = self.sql_templates["profit_analysis"].format(limit=limit)
            template_used = "profit_analysis"
        elif "top_products" in intents or ("product" in intents and "rating" in intents):
            if "rating" in intents:
                sql = self.sql_templates["top_products_rating"].format(limit=limit)
                template_used = "top_products_rating"
            else:
                sql = self.sql_templates["top_products_revenue"].format(limit=limit)
                template_used = "top_products_revenue"
        elif "top_customers" in intents or ("customer" in intents and "revenue" in intents):
            sql = self.sql_templates["top_customers"].format(limit=limit)
            template_used = "top_customers"
        elif "customer" in intents:
            sql = self.sql_templates["customer_segments"]
            template_used = "customer_segments"
        elif "inventory" in intents:
            sql = self.sql_templates["inventory_status"]
            template_used = "inventory_status"
        elif "payment" in intents:
            sql = self.sql_templates["payment_analysis"]
            template_used = "payment_analysis"
        elif "refund" in intents:
            sql = self.sql_templates["refund_analysis"]
            template_used = "refund_analysis"
        elif "orders" in intents:
            sql = self.sql_templates["order_status_breakdown"]
            template_used = "order_status_breakdown"
        elif "revenue" in intents:
            sql = self.sql_templates["revenue_trend"]
            template_used = "revenue_trend"
        elif "trend" in intents:
            sql = self.sql_templates["revenue_trend"]
            template_used = "revenue_trend"
        elif "category" in intents:
            sql = self.sql_templates["revenue_by_category"]
            template_used = "revenue_by_category"
        elif "region" in intents:
            sql = self.sql_templates["revenue_by_region"]
            template_used = "revenue_by_region"
        elif "product" in intents:
            sql = self.sql_templates["top_products_revenue"].format(limit=limit)
            template_used = "top_products_revenue"
        else:
            sql = self.sql_templates["general_stats"]
            template_used = "general_stats"

        reasoning_steps.append(f"3. Selected template: {template_used}")
        reasoning_steps.append(f"4. Applied limit: {limit}")

        # Find most relevant few-shot example
        best_example = None
        prompt_lower = prompt.lower()
        for ex in SQL_FEW_SHOT_EXAMPLES:
            if any(word in prompt_lower for word in ex["input"].lower().split()):
                best_example = ex
                break

        return {
            "sql": sql,
            "template": template_used,
            "intents": intents,
            "entities": entities,
            "reasoning": reasoning_steps,
            "system_prompt": SQL_SYSTEM_PROMPT,
            "similar_example": best_example,
            "prompt_technique": "Few-shot + Chain-of-thought + Role prompting"
        }

    def generate_transform(self, prompt: str) -> Dict:
        """Generate data transformation code from natural language description."""
        prompt_lower = prompt.lower()
        matches = []

        keywords_map = {
            "clean_nulls": ["null", "missing", "empty", "clean", "fill", "nan"],
            "normalize_prices": ["normalize", "price", "standardize", "decimal", "outlier"],
            "aggregate_revenue": ["aggregate", "group", "summarize", "revenue", "total", "sum"],
            "customer_rfm": ["rfm", "segment", "recency", "frequency", "monetary", "customer segment"],
            "deduplicate": ["duplicate", "dedup", "unique", "remove duplicate", "distinct"],
            "enrich_dates": ["date", "temporal", "time", "day of week", "quarter", "month", "year", "feature"],
        }

        for template_key, keywords in keywords_map.items():
            score = sum(1 for kw in keywords if kw in prompt_lower)
            if score > 0:
                matches.append((template_key, score))

        matches.sort(key=lambda x: x[1], reverse=True)

        if matches:
            selected = self.transform_templates[matches[0][0]]
        else:
            selected = self.transform_templates["clean_nulls"]

        return {
            "transformation": selected,
            "matched_keywords": [m[0] for m in matches],
            "prompt_technique": "Keyword matching + Template rendering",
            "reasoning": [
                f"Analyzed prompt: '{prompt}'",
                f"Matched templates: {[m[0] for m in matches]}",
                f"Selected: {selected['name']}",
            ]
        }

    def generate_quality_rules(self, prompt: str) -> Dict:
        """Generate data quality rules based on context."""
        prompt_lower = prompt.lower()
        selected_categories = []

        category_keywords = {
            "completeness": ["complete", "null", "missing", "empty", "required"],
            "consistency": ["consistent", "match", "correct", "valid calculation", "logic"],
            "validity": ["valid", "range", "boundary", "limit", "constraint", "format"],
            "uniqueness": ["unique", "duplicate", "distinct", "primary key"],
            "timeliness": ["fresh", "recent", "timely", "date range", "outdated"],
        }

        for cat_key, keywords in category_keywords.items():
            if any(kw in prompt_lower for kw in keywords):
                selected_categories.append(cat_key)

        if not selected_categories:
            selected_categories = list(self.quality_rules.keys())

        rules = {cat: self.quality_rules[cat] for cat in selected_categories}

        return {
            "quality_rules": rules,
            "categories_checked": selected_categories,
            "total_rules": sum(len(r["rules"]) for r in rules.values()),
            "prompt_technique": "Context-aware rule selection",
            "reasoning": [
                f"Analyzed quality context: '{prompt}'",
                f"Selected categories: {selected_categories}",
                f"Total rules generated: {sum(len(r['rules']) for r in rules.values())}",
            ]
        }


# Singleton instance
engine = PromptEngine()
