import os

from kaggle.api.kaggle_api_extended import KaggleApi


def download_dataset():
    api = KaggleApi()
    api.authenticate()

    dataset = "anoopjohny/consumer-complaint-database"
    download_path = "./data"
    data_path = os.path.join(download_path, "complaints.csv")

    # Ensure the data directory exists
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Check if the dataset file already exists
    if os.path.exists(data_path):
        print(f"Dataset already exists at {data_path}. Skipping download.")
    else:
        # Download the dataset files
        print("Downloading dataset...")
        api.dataset_download_files(dataset, path=download_path, unzip=True)

        # Verify the file was downloaded
        if not os.path.exists(data_path):
            raise FileNotFoundError("Dataset download failed.")
        else:
            print("Dataset downloaded and extracted successfully.")

    return data_path


if __name__ == "__main__":
    data_path = download_dataset()
    print(f"Data path: {data_path}")
