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
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # Must be SERVICE ROLE KEY

    if not url or not key:
        raise ValueError("Supabase URL or Key not found in environment variables")

    return create_client(url, key)


# -----------------------------------------
# 2. Create Table (through RPC exec_sql)
# -----------------------------------------
def create_table_if_not_exists():
    supabase = get_supabase_client()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS iris_data (
        id BIGSERIAL PRIMARY KEY,
        sepal_length FLOAT,
        sepal_width FLOAT,
        petal_length FLOAT,
        petal_width FLOAT,  
        species TEXT,
        sepal_ratio FLOAT,
        petal_ratio FLOAT,
        is_petal_long INT
    );
    """

    try:
        supabase.rpc("exec_sql", {"query": create_table_sql}).execute()
        print("✅ Table 'iris_data' ensured to exist.")
    except Exception as e:
        print("❌ Error creating table:", e)
        print("⚠️ Table was NOT created. Check RPC permissions.")


# -----------------------------------------
# 3. Load CSV into Supabase
# -----------------------------------------
def load_data_supabase(staged_path: str, table_name: str = "iris_data"):

    # If path is relative → convert to full path
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), staged_path)
        )
        print(f"Looking for data file at: {staged_path}")

    # Check file exists
    if not os.path.exists(staged_path):
        print(f"❌ ERROR: File not found at: {staged_path}")
        print("Run transform_iris.py first.")
        return

    try:
        supabase = get_supabase_client()

        df = pd.read_csv(staged_path)
        total_rows = len(df)
        batch_size = 50

        print(f"🚀 Loading {total_rows} rows into '{table_name}'...")

        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size].copy()

            # Convert NaN → None for Supabase
            batch = batch.where(pd.notnull(batch), None)

            # Convert to list of dictionaries
            records = batch.to_dict("records")

            try:
                supabase.table(table_name).insert(records).execute()
                print(f"✅ Inserted rows {i+1} to {min(i+batch_size, total_rows)}")
            except Exception as e:
                print(f"❌ Error inserting batch starting at row {i+1}: {e}")

        print("🎉 Data loading completed successfully!")

    except Exception as e:
        print(f"❌ Error initializing Supabase client: {e}")
        print("Data loading aborted.")


# -----------------------------------------
# 4. Main Runner
# -----------------------------------------
if __name__ == "__main__":

    # FIXED: No more "..." in path!
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_csv_path = os.path.join(base_dir, "data", "staged", "iris_transformed.csv")

    create_table_if_not_exists()
    load_data_supabase(staged_csv_path)

    
