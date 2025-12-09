import pandas as pd
import os
from Extract_titanic import extract_data


def transform_data(raw_path):

    # -------------------------------
    # 1. Base & staged directories
    # -------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))
    staged_dir = os.path.join(base_dir, "..", "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    # Load raw Titanic dataset
    df = pd.read_csv(raw_path)

    # -------------------------------
    # 2. Handling Missing Values
    # -------------------------------
    numeric_cols = [
        "age", "fare", "sibsp", "parch"
    ]

    # Fill numeric missing values with median
    for col in numeric_cols:
        if col in df.columns:
            df[col].fillna(df[col].median(), inplace=True)

    # Fill categorical missing values with mode
    categorical_cols = [
        "sex", "embarked", "class", "who", "deck",
        "embark_town", "alive", "alone"
    ]

    for col in categorical_cols:
        if col in df.columns:
            df[col].fillna(df[col].mode()[0], inplace=True)

    # -------------------------------
    # 3. Feature Engineering
    # -------------------------------
    # Example feature: is_adult
    df["is_adult"] = df["age"].apply(lambda x: 1 if x >= 18 else 0)

    # Example feature: fare_per_person
    df["fare_per_person"] = df.apply(
        lambda row: row["fare"] / (row["sibsp"] + row["parch"] + 1)
        if row["fare"] is not None else None,
        axis=1
    )

    # -------------------------------
    # 4. Drop unnecessary columns
    # -------------------------------
    df.drop(columns=[], inplace=True, errors="ignore")

    # -------------------------------
    # 5. Save transformed data
    # -------------------------------
    staged_path = os.path.join(staged_dir, "titanic_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"Transformed Titanic data saved to: {staged_path}")
    return staged_path


# -------------------------------
# 6. Main runner
# -------------------------------
if __name__ == "__main__":
    raw_path = extract_data()
    transform_data(raw_path)
