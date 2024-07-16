from fastapi import APIRouter, HTTPException, Query
from config import (
    DATASET,
    DOWNLOAD_PATH,
    DATA_FILE_NAME,
    DATA_PATH,
    DATABASE_URI,
    MAIN_TABLE_NAME,
    TEMP_TABLE_NAME,
    SCHEMA_NAME,
    MIN_BATCH_SIZE,
    MAX_BATCH_SIZE,
)
from extract.download_data import download_dataset
from load.load_data import load_all_data_to_database

router = APIRouter()


@router.post("/complaints/download")
def download_complaints():
    try:
        data_path = download_dataset(
            kaggle_dataset=DATASET,
            download_path=DOWNLOAD_PATH,
            data_file_name=DATA_FILE_NAME,
        )
        return {"message": "Dataset downloaded successfully.", "data_path": data_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/complaints/load_all")
def load_all_complaints(
    batch_size: int = Query(MAX_BATCH_SIZE, ge=MIN_BATCH_SIZE, le=MAX_BATCH_SIZE)
):
    try:
        load_all_data_to_database(
            data_path=DATA_PATH,
            database_uri=DATABASE_URI,
            main_table_name=MAIN_TABLE_NAME,
            temp_table_name=TEMP_TABLE_NAME,
            schema_name=SCHEMA_NAME,
            batch_size=batch_size,
        )
        return {"message": "Data loaded successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/complaints/load_batch")
def load_batch_complaints(
    batch_size: int = Query(MAX_BATCH_SIZE, ge=MIN_BATCH_SIZE, le=MAX_BATCH_SIZE)
):
    pass
