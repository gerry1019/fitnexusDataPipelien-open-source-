import os
import pandas as pd
from sqlalchemy import create_engine, text

# --------------------------------------------
# Dynamic path setup (same as your main loader)
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

engine = create_engine(DB_URI, connect_args={"ssl": {"ssl": {}}})

# --------------------------------------------
# Create translation table (with required PK)
# --------------------------------------------
create_table_sql = """
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product_category_name VARCHAR(255),
    product_category_name_english VARCHAR(255)
);
"""

with engine.begin() as conn:
    conn.execute(text(create_table_sql))

print("✅ Verified/Created product_category_name_translation table")

# --------------------------------------------
# Load CSV
# --------------------------------------------
file_path = os.path.join(data_dir, "product_category_name_translation.csv")
print(f"📥 Loading {file_path} → product_category_name_translation")

if not os.path.exists(file_path):
    raise FileNotFoundError(f"❌ CSV file not found: {file_path}")

df = pd.read_csv(file_path)
df.columns = [col.strip().lower() for col in df.columns]

# Remove duplicates
df.drop_duplicates(inplace=True)

# --------------------------------------------
# Insert data
# --------------------------------------------
try:
    df.to_sql(
        "product_category_name_translation",
        con=engine,
        if_exists="append",
        index=False
    )
    print(f"✅ Loaded {len(df)} rows → product_category_name_translation\n")

except Exception as e:
    print(f"❌ Failed to load translation table: {e}\n")

print("🎉 Translation data loaded successfully into MySQL! ✅")
