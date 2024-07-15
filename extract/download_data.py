import os
from kaggle.api.kaggle_api_extended import KaggleApi


def download_dataset(kaggle_dataset, download_path, data_file_name):
    """
    Downloads the consumer complaint dataset from Kaggle if it does not already exist locally.

    :param kaggle_dataset: The Kaggle dataset identifier.
    :param download_path: The directory where the dataset will be downloaded.
    :param data_file_name: The name of the dataset file.
    :return: The path to the downloaded dataset file.
    :raises FileNotFoundError: If the dataset download fails and the file is not found at the
            expected location.
    """
    api = KaggleApi()
    api.authenticate()

    data_path = os.path.join(download_path, data_file_name)

    # Ensure the data directory exists
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Check if the dataset file already exists
    if os.path.exists(data_path):
        print(f"Dataset already exists at {data_path}. Skipping download.")
    else:
        # Download the dataset files
        print("Downloading dataset...")
        api.dataset_download_files(kaggle_dataset, path=download_path, unzip=True)

        # Verify the file was downloaded
        if not os.path.exists(data_path):
            raise FileNotFoundError("Dataset download failed.")
        else:
            print("Dataset downloaded and extracted successfully.")

    return data_path


if __name__ == "__main__":
    # Configuration
    KAGGLE_DATASET = "anoopjohny/consumer-complaint-database"
    DOWNLOAD_PATH = "./data"
    DATA_FILE_NAME = "complaints.csv"

    # Download the dataset
    data_path = download_dataset(KAGGLE_DATASET, DOWNLOAD_PATH, DATA_FILE_NAME)
    print(f"Data path: {data_path}")
