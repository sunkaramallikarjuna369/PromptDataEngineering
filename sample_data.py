"""
Sample E-Commerce Data Generator
Generates realistic orders, products, customers, and categories for DataForge AI.
"""

import sqlite3
import random
import os
from datetime import datetime, timedelta

CATEGORIES = [
    {"id": 1, "name": "Electronics", "margin": 0.22},
    {"id": 2, "name": "Clothing & Apparel", "margin": 0.45},
    {"id": 3, "name": "Home & Kitchen", "margin": 0.35},
    {"id": 4, "name": "Books & Media", "margin": 0.40},
    {"id": 5, "name": "Sports & Outdoors", "margin": 0.30},
    {"id": 6, "name": "Beauty & Health", "margin": 0.50},
    {"id": 7, "name": "Toys & Games", "margin": 0.38},
    {"id": 8, "name": "Automotive", "margin": 0.25},
]

PRODUCTS = [
    {"name": "Wireless Bluetooth Headphones", "category_id": 1, "price": 79.99, "cost": 35.00},
    {"name": "Smart Watch Pro", "category_id": 1, "price": 249.99, "cost": 120.00},
    {"name": "USB-C Hub Adapter", "category_id": 1, "price": 34.99, "cost": 12.00},
    {"name": "Portable Charger 20000mAh", "category_id": 1, "price": 44.99, "cost": 18.00},
    {"name": "Noise Cancelling Earbuds", "category_id": 1, "price": 129.99, "cost": 55.00},
    {"name": "4K Action Camera", "category_id": 1, "price": 199.99, "cost": 90.00},
    {"name": "Men's Running Jacket", "category_id": 2, "price": 89.99, "cost": 32.00},
    {"name": "Women's Yoga Pants", "category_id": 2, "price": 49.99, "cost": 15.00},
    {"name": "Cotton Graphic T-Shirt", "category_id": 2, "price": 24.99, "cost": 8.00},
    {"name": "Leather Crossbody Bag", "category_id": 2, "price": 64.99, "cost": 22.00},
    {"name": "Winter Puffer Coat", "category_id": 2, "price": 149.99, "cost": 55.00},
    {"name": "Stainless Steel Cookware Set", "category_id": 3, "price": 199.99, "cost": 85.00},
    {"name": "Robot Vacuum Cleaner", "category_id": 3, "price": 299.99, "cost": 140.00},
    {"name": "Memory Foam Pillow (2-Pack)", "category_id": 3, "price": 39.99, "cost": 14.00},
    {"name": "Air Purifier HEPA Filter", "category_id": 3, "price": 159.99, "cost": 65.00},
    {"name": "LED Desk Lamp", "category_id": 3, "price": 29.99, "cost": 10.00},
    {"name": "Python Data Science Handbook", "category_id": 4, "price": 44.99, "cost": 18.00},
    {"name": "The Art of SQL", "category_id": 4, "price": 39.99, "cost": 16.00},
    {"name": "Data Engineering with Python", "category_id": 4, "price": 49.99, "cost": 20.00},
    {"name": "Machine Learning Yearning", "category_id": 4, "price": 29.99, "cost": 12.00},
    {"name": "Adjustable Dumbbell Set", "category_id": 5, "price": 179.99, "cost": 80.00},
    {"name": "Camping Tent 4-Person", "category_id": 5, "price": 129.99, "cost": 50.00},
    {"name": "Yoga Mat Premium", "category_id": 5, "price": 34.99, "cost": 12.00},
    {"name": "Cycling Helmet", "category_id": 5, "price": 59.99, "cost": 22.00},
    {"name": "Vitamin C Serum", "category_id": 6, "price": 24.99, "cost": 6.00},
    {"name": "Electric Toothbrush", "category_id": 6, "price": 69.99, "cost": 25.00},
    {"name": "Organic Shampoo Set", "category_id": 6, "price": 32.99, "cost": 10.00},
    {"name": "Sunscreen SPF 50", "category_id": 6, "price": 14.99, "cost": 4.00},
    {"name": "LEGO Architecture Set", "category_id": 7, "price": 89.99, "cost": 40.00},
    {"name": "Board Game Collection", "category_id": 7, "price": 49.99, "cost": 18.00},
    {"name": "RC Racing Car", "category_id": 7, "price": 44.99, "cost": 16.00},
    {"name": "Car Phone Mount", "category_id": 8, "price": 19.99, "cost": 5.00},
    {"name": "Dash Cam HD", "category_id": 8, "price": 79.99, "cost": 30.00},
    {"name": "Tire Pressure Gauge Digital", "category_id": 8, "price": 12.99, "cost": 4.00},
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra", "Steven", "Ashley",
    "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna", "Kenneth", "Michelle",
    "Aisha", "Raj", "Priya", "Wei", "Yuki", "Carlos", "Fatima", "Ahmed", "Sofia", "Ivan",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Patel", "Kumar", "Singh", "Chen", "Wang", "Kim", "Tanaka", "Müller", "Ivanov", "Santos",
]

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East & Africa"]
CITIES = {
    "North America": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Toronto", "Vancouver"],
    "Europe": ["London", "Paris", "Berlin", "Madrid", "Rome", "Amsterdam", "Stockholm"],
    "Asia Pacific": ["Tokyo", "Sydney", "Singapore", "Mumbai", "Seoul", "Shanghai", "Auckland"],
    "Latin America": ["São Paulo", "Mexico City", "Buenos Aires", "Lima", "Bogotá"],
    "Middle East & Africa": ["Dubai", "Cape Town", "Nairobi", "Istanbul", "Cairo"],
}

ORDER_STATUSES = ["Completed", "Processing", "Shipped", "Cancelled", "Refunded"]
PAYMENT_METHODS = ["Credit Card", "PayPal", "Debit Card", "Apple Pay", "Google Pay", "Bank Transfer"]


def create_tables(db_path: str):
    """Create all database tables."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS order_items")
    c.execute("DROP TABLE IF EXISTS orders")
    c.execute("DROP TABLE IF EXISTS customers")
    c.execute("DROP TABLE IF EXISTS products")
    c.execute("DROP TABLE IF EXISTS categories")
    c.execute("DROP TABLE IF EXISTS datasets")
    c.execute("DROP TABLE IF EXISTS pipeline_runs")

    c.execute("""
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            margin REAL NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category_id INTEGER,
            price REAL NOT NULL,
            cost REAL NOT NULL,
            stock INTEGER DEFAULT 100,
            rating REAL DEFAULT 4.0,
            reviews_count INTEGER DEFAULT 0,
            created_at TEXT,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
    """)

    c.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE,
            region TEXT,
            city TEXT,
            signup_date TEXT,
            lifetime_value REAL DEFAULT 0,
            order_count INTEGER DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            payment_method TEXT,
            subtotal REAL,
            discount REAL DEFAULT 0,
            tax REAL DEFAULT 0,
            total REAL,
            region TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    """)

    c.execute("""
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)

    c.execute("""
        CREATE TABLE datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            source TEXT NOT NULL,
            row_count INTEGER,
            column_count INTEGER,
            created_at TEXT,
            status TEXT DEFAULT 'active'
        )
    """)

    c.execute("""
        CREATE TABLE pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            records_processed INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            stage TEXT DEFAULT 'idle'
        )
    """)

    conn.commit()
    conn.close()


def seed_data(db_path: str):
    """Seed the database with sample e-commerce data."""
    random.seed(42)
    create_tables(db_path)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Insert categories
    for cat in CATEGORIES:
        c.execute("INSERT INTO categories (id, name, margin) VALUES (?, ?, ?)",
                  (cat["id"], cat["name"], cat["margin"]))

    # Insert products
    product_ids = []
    for i, prod in enumerate(PRODUCTS, 1):
        rating = round(random.uniform(3.2, 5.0), 1)
        reviews = random.randint(10, 2500)
        stock = random.randint(0, 500)
        created = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat()
        c.execute("""INSERT INTO products (name, category_id, price, cost, stock, rating, reviews_count, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                  (prod["name"], prod["category_id"], prod["price"], prod["cost"], stock, rating, reviews, created))
        product_ids.append(i)

    # Insert customers
    customer_ids = []
    emails_used = set()
    for i in range(200):
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        email_base = f"{fn.lower()}.{ln.lower()}"
        email = f"{email_base}{random.randint(1,999)}@example.com"
        while email in emails_used:
            email = f"{email_base}{random.randint(1,9999)}@example.com"
        emails_used.add(email)
        region = random.choice(REGIONS)
        city = random.choice(CITIES[region])
        signup = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
        c.execute("""INSERT INTO customers (first_name, last_name, email, region, city, signup_date)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (fn, ln, email, region, city, signup))
        customer_ids.append(i + 1)

    # Insert orders & order_items
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 12, 31)
    total_days = (end_date - start_date).days

    for order_num in range(500):
        cust_id = random.choice(customer_ids)
        order_date = (start_date + timedelta(days=random.randint(0, total_days))).strftime("%Y-%m-%d")
        status = random.choices(ORDER_STATUSES, weights=[60, 15, 12, 8, 5])[0]
        payment = random.choice(PAYMENT_METHODS)
        region = random.choice(REGIONS)

        # Generate 1-5 items per order
        num_items = random.choices([1, 2, 3, 4, 5], weights=[35, 30, 20, 10, 5])[0]
        selected_products = random.sample(product_ids, min(num_items, len(product_ids)))

        subtotal = 0
        items = []
        for pid in selected_products:
            prod = PRODUCTS[pid - 1]
            qty = random.randint(1, 4)
            unit_price = prod["price"]
            line_total = round(unit_price * qty, 2)
            subtotal += line_total
            items.append((pid, qty, unit_price, line_total))

        discount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]), 2)
        tax = round((subtotal - discount) * 0.08, 2)
        total = round(subtotal - discount + tax, 2)

        c.execute("""INSERT INTO orders (customer_id, order_date, status, payment_method, subtotal, discount, tax, total, region)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (cust_id, order_date, status, payment, subtotal, discount, tax, total, region))
        order_id = c.lastrowid

        for pid, qty, up, lt in items:
            c.execute("""INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
                         VALUES (?, ?, ?, ?, ?)""",
                      (order_id, pid, qty, up, lt))

    # Update customer lifetime values
    c.execute("""
        UPDATE customers SET
            lifetime_value = COALESCE((SELECT SUM(total) FROM orders WHERE orders.customer_id = customers.id AND orders.status != 'Cancelled'), 0),
            order_count = COALESCE((SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id), 0)
    """)

    # Insert default dataset record
    c.execute("""INSERT INTO datasets (name, source, row_count, column_count, created_at, status)
                 VALUES (?, ?, ?, ?, ?, ?)""",
              ("E-Commerce Sample", "seed", 500, 9, datetime.now().isoformat(), "active"))

    conn.commit()
    conn.close()
    return {"message": "Seeded 8 categories, 34 products, 200 customers, 500 orders"}


if __name__ == "__main__":
    seed_data("dataforge.db")
    print("✅ Sample data seeded successfully!")
