import pandas as pd
import os

raw_path = "data/raw/"
bronze_path = "data/bronze/"

os.makedirs(bronze_path, exist_ok=True)

for file in os.listdir(raw_path):
    if file.endswith(".csv"):
        df = pd.read_csv(os.path.join(raw_path, file))
        df.to_csv(os.path.join(bronze_path, file), index=False)
        print(f"Ingested {file} to Bronze layer")
