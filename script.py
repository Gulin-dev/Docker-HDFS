import os
import pandas as pd
from pyarrow import parquet
from pyarrow import Table
from pathlib import Path

RAW_DATA_DIR = Path("/app/data/raw")
NORMALIZED_DATA_DIR = Path("/app/data/normalized")

NORMALIZED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def normalize_world_events(file_path):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.replace(" ", "_")
    try:
        df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Date']].astype(str).agg('-'.join, axis=1),
                                    format='%Y-%m-%d', errors='coerce')
    except Exception as e:
        print(f"Error parsing dates in world events: {e}")
    df.drop(columns=['Year', 'Month', 'Date'], inplace=True, errors='ignore')
    return df


def normalize_car_accidents(file_path):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.replace(" ", "_")
    try:
        df['Accident_Date'] = pd.to_datetime(df.iloc[:, 1], format='%Y-%m-%d', errors='coerce')
    except Exception as e:
        print(f"Error parsing accident dates: {e}")
    df.drop(columns=[df.columns[1]], inplace=True, errors='ignore')
    return df


def save_to_parquet(df, file_name):
    table = Table.from_pandas(df)
    parquet.write_table(table, NORMALIZED_DATA_DIR / file_name)


def upload_to_hdfs(local_path, hdfs_path):
    hdfs_command = f"hdfs dfs -mkdir -p {hdfs_path} && hdfs dfs -put -f {local_path} {hdfs_path}"
    exit_code = os.system(hdfs_command)
    if exit_code != 0:
        print(f"HDFS upload failed for {local_path}")
    else:
        print(f"Successfully uploaded {local_path} to HDFS")


def main():
    world_events_file = RAW_DATA_DIR / "World Important Dates.csv"
    car_accidents_file = RAW_DATA_DIR / "Road Accident Data.csv"
    hdfs_target_dir = "/user/hdfs/normalized_data/"

    if world_events_file.exists():
        world_events_df = normalize_world_events(world_events_file)
        save_to_parquet(world_events_df, "World_Important_Dates.parquet")
        upload_to_hdfs(NORMALIZED_DATA_DIR / "World_Important_Dates.parquet", hdfs_target_dir)
        print("World events data processed, saved, and uploaded to HDFS.")

    if car_accidents_file.exists():
        car_accidents_df = normalize_car_accidents(car_accidents_file)
        save_to_parquet(car_accidents_df, "car_accidents.parquet")
        upload_to_hdfs(NORMALIZED_DATA_DIR / "car_accidents.parquet", hdfs_target_dir)
        print("Car accidents data processed, saved, and uploaded to HDFS.")

    print("Listing files in HDFS:")
    os.system(f"hdfs dfs -ls {hdfs_target_dir}")


if __name__ == "__main__":
    main()