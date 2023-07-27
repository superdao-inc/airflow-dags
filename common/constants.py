from pathlib import Path

DATA_DIR = "/home/airflow"
TEMP_DIR = DATA_DIR if Path(DATA_DIR).exists() else None
