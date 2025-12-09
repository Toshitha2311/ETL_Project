import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv


# -----------------------------------------
# 1. Initialize Supabase Client
# -----------------------------------------
def get_supabase_client():
    load_dotenv()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # Must use SERVICE ROLE KEY

    if not url or not key:
        raise ValueError("Supabase URL or Service Role Key missing in environment")

    return create_client(url, key)


# -----------------------------------------
# 2. Create Table
# -----------------------------------------
def create_table_if_not_exists():
    supabase = get_supabase_client()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS titanic_data (
        id BIGSERIAL PRIMARY KEY,
        survived INT,
        pclass INT,
        sex TEXT,
        age FLOAT,
        sibsp INT,
        parch INT,
        fare FLOAT,
        embarked TEXT,
        class TEXT,
        who TEXT,
        adult_male INT,
        deck TEXT,
        embark_town TEXT,
        alive TEXT,
        alone INT,
        is_adult INT,
        fare_per_person FLOAT
    );
    """

    supabase.rpc("exec_sql", {"query": create_table_sql}).execute()
    print("✅ Table 'titanic_data' ensured to exist.")


# -----------------------------------------
# 3. Load Titanic CSV
# -----------------------------------------
def load_titanic_data(staged_path: str, table_name: str = "titanic_data"):

    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), staged_path)
        )

    if not os.path.exists(staged_path):
        print(f"❌ ERROR: File not found: {staged_path}")
        return

    supabase = get_supabase_client()

    df = pd.read_csv(staged_path)

    # -----------------------------------------
    # FIX: Convert boolean-like strings → integers
    # -----------------------------------------
    bool_cols = ["adult_male", "alone", "is_adult"]

    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map(
                {"true": 1, "false": 0, "1": 1, "0": 0}
            ).fillna(0).astype(int)

    # -----------------------------------------
    # Clean NaN / inf → None
    # -----------------------------------------
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notnull(df), None)

    total_rows = len(df)
    batch_size = 50

    print(f"🚀 Loading {total_rows} rows into '{table_name}'...")

    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i+batch_size].copy()
        batch = batch.where(pd.notnull(batch), None)
        records = batch.to_dict("records")

        try:
            supabase.table(table_name).insert(records).execute()
            print(f"✅ Inserted rows {i+1} to {min(i+batch_size, total_rows)}")
        except Exception as e:
            print(f"❌ Error inserting batch at row {i+1}: {e}")

    print("🎉 Titanic data loaded successfully!")


# -----------------------------------------
# 4. Main Runner
# -----------------------------------------
if __name__ == "__main__":

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_csv_path = os.path.join(base_dir, "data", "staged", "titanic_transformed.csv")

    create_table_if_not_exists()
    load_titanic_data(staged_csv_path)
