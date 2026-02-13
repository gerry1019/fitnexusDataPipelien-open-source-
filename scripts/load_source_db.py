import os
import pandas as pd
import zipfile
from sqlalchemy import create_engine, text

# --------------------------------------------
# Dynamic path setup
# --------------------------------------------
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(base_dir, "data")

print("📂 Data folder path:", data_dir)
print("📁 Files in data folder:", os.listdir(data_dir))

# --------------------------------------------
# MySQL shared (Aiven)
# --------------------------------------------
APP_DB_CONFIG = {
    "host": os.getenv("APP_DB_HOST"),
    "port": os.getenv("APP_DB_PORT"),
    "user": os.getenv("APP_DB_USER"),
    "password": os.getenv("APP_DB_PASS"),
    "dbname": os.getenv("APP_DB_NAME"),
}

DB_URI = (
    f"mysql+pymysql://{APP_DB_CONFIG['user']}:"
    f"{APP_DB_CONFIG['password']}@"
    f"{APP_DB_CONFIG['host']}:"
    f"{APP_DB_CONFIG['port']}/"
    f"{APP_DB_CONFIG['dbname']}"
)

# ✅ Use SSL shared
engine = create_engine(DB_URI, connect_args={"ssl": {"ssl": {}}})

# --------------------------------------------
# Helper to read CSV or ZIP
# --------------------------------------------
def read_csv_or_zip(path):
    """Reads CSV directly or extracts CSV from .zip"""
    if os.path.exists(path):
        return pd.read_csv(path)
    elif os.path.exists(path + ".zip"):
        with zipfile.ZipFile(path + ".zip") as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                return pd.read_csv(f)
    else:
        print(f"❌ File not found: {path} or {path}.zip")
        return None

# --------------------------------------------
# File → Table mapping
# --------------------------------------------
tables = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
}

# --------------------------------------------
# Table primary key mapping
# --------------------------------------------
primary_keys = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "id",
    "order_payments": "id",
    "order_reviews": "review_id",
    "products": "product_id",
    "sellers": "seller_id",
    "geolocation": "id"
}

# --------------------------------------------
# Optional: Create schema if not exists
# --------------------------------------------
def create_schema_if_missing(engine):
    schema_sql = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_unique_id VARCHAR(50),
        customer_zip_code_prefix INT,
        customer_city VARCHAR(100),
        customer_state VARCHAR(10)
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50),
        order_status VARCHAR(50),
        order_purchase_timestamp DATETIME,
        order_approved_at DATETIME,
        order_delivered_carrier_date DATETIME,
        order_delivered_customer_date DATETIME,
        order_estimated_delivery_date DATETIME
    );

    CREATE TABLE IF NOT EXISTS order_items (
        id BIGINT PRIMARY KEY,
        order_id VARCHAR(50),
        order_item_id INT,
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        shipping_limit_date DATETIME,
        price FLOAT,
        freight_value FLOAT
    );

    CREATE TABLE IF NOT EXISTS order_payments (
        id BIGINT PRIMARY KEY,
        order_id VARCHAR(50),
        payment_sequential INT,
        payment_type VARCHAR(50),
        payment_installments INT,
        payment_value FLOAT
    );

    CREATE TABLE IF NOT EXISTS order_reviews (
        review_id VARCHAR(50) PRIMARY KEY,
        order_id VARCHAR(50),
        review_score INT,
        review_comment_title TEXT,
        review_comment_message TEXT,
        review_creation_date DATETIME,
        review_answer_timestamp DATETIME
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(50) PRIMARY KEY,
        product_category_name VARCHAR(100),
        product_name_lenght INT,
        product_description_lenght INT,
        product_photos_qty INT,
        product_weight_g INT,
        product_length_cm INT,
        product_height_cm INT,
        product_width_cm INT
    );

    CREATE TABLE IF NOT EXISTS sellers (
        seller_id VARCHAR(50) PRIMARY KEY,
        seller_zip_code_prefix INT,
        seller_city VARCHAR(100),
        seller_state VARCHAR(10)
    );

    CREATE TABLE IF NOT EXISTS geolocation (
        id BIGINT PRIMARY KEY,
        geolocation_zip_code_prefix INT,
        geolocation_lat FLOAT,
        geolocation_lng FLOAT,
        geolocation_city VARCHAR(100),
        geolocation_state VARCHAR(10)
    );
    """
    with engine.begin() as conn:
        for stmt in schema_sql.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    print("✅ Verified/Created all tables with primary keys.")

# Create schema if missing
create_schema_if_missing(engine)

# --------------------------------------------
# Load loop
# --------------------------------------------
for file, table in tables.items():
    file_path = os.path.join(data_dir, file)
    print(f"📥 Loading {file_path} → {table}")

    df = read_csv_or_zip(file_path)
    if df is None:
        continue

    df.columns = [c.strip().lower() for c in df.columns]
    df.drop_duplicates(inplace=True)

    # Add synthetic primary key column if missing
    pk = primary_keys.get(table)
    if pk not in df.columns:
        df.insert(0, pk, range(1, len(df) + 1))

    # Convert datetime-like columns
    for col in df.columns:
        if "date" in col or "timestamp" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Append data instead of replacing table
    try:
        df.to_sql(table, engine, if_exists="append", index=False)
        print(f"✅ Loaded {len(df)} rows → {table}\n")
    except Exception as e:
        print(f"❌ Failed to load {table}: {e}\n")

print("🎉 All Olist data loaded successfully into MySQL database: olist_source ✅")
