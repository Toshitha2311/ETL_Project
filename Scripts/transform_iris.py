import pandas as pd
import os
import seaborn as sns
from Extract_iris import extract_data
def transform_data(raw_path):
    base_dir=os.path.dirname(os.path.abspath(__file__))
    staged_dir=os.path.join(base_dir,"..","data","staged")
    os.makedirs(staged_dir,exist_ok=True)
    df=pd.read_csv(raw_path)
    #1.Handling the missing values
    # numeric_cols=df.select_dtypes(include=['float64','int64']).columns
    numeric_cols=['sepal_length','sepal_width','petal_length','petal_width']
    #Filling the missing values with median
    for col in numeric_cols:
        med=df[col].median()
        df[col].fillna(med,inplace=True)
    #.Encoding categorical variables
    df["species"]=df["species"].fillna(df["species"].mode()[0])
    #2.Feature Engineering(dividing or combning the columns)
    df["sepal_ratio"]=df["sepal_length"]/df["sepal_width"]
    df["petal_ratio"]=df["petal_length"]/df["petal_width"]
    df["is_petal_long"]=df["petal_length"].apply(lambda x:1 if x>df["petal_length"].median()else 0)
    #3.Dropping unnecessary Columns
    df.drop(columns=[],inplace=True,errors="ignore")
    #4.Saving the transformed data
    staged_path=os.path.join(staged_dir,"iris_transformed.csv")
    df.to_csv(staged_path,index=False)
    print(f"Transformed data saved to {staged_path}")
    return staged_path
if __name__=="__main__":
    raw_path=extract_data()
    transform_data(raw_path)


    