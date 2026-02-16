import requests
from pathlib import Path
import zipfile
import os

class DownloadError(Exception):
    pass

def download_citibike_data(year, month=None, output_directory="data/raw"):
    """
    download one month of citibike data

    -- arguments --
        year: year value as integer (e.g., 2025)
        month: month value as integer (e.g., 11)
        output_directory: directory where downloaded data will be stored
    """

    # older citibike data is stored in yearly folders
    if month is not None:
        file_name = f"{year}{month:02}-citibike-tripdata.zip"
        year_month = f"{year}/{month:02}"
    else:
        file_name = f"{year}-citibike-tripdata.zip"
        year_month = f"{year}"

    # citibike url format for yearly and monthly data
    url = f"https://s3.amazonaws.com/tripdata/{file_name}"

    # establish full directory path for zip file download destination
    zip_path = Path(output_directory) / year_month / file_name
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        download_size = int(response.headers.get("content-length", 0))
        print(f"download size of {(download_size / (1024 * 1024)):.2f} megabytes")

        # metadata progression logging
        download_count = 0
        progress_interval = 10 * 1024 * 1024
        next_log = progress_interval

        # open zip file and stream the fetched bytes into it
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=10*1024*1024):
                f.write(chunk)
                download_count += len(chunk)
                if download_count >= next_log:
                    print(f"downloaded {(download_count / (1024 * 1024)):.2f} megabytes")
                    next_log += progress_interval
            print(f"download complete {url}")
        
        # unzip downloaded zip file
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(Path(output_directory) / year_month)

        # remove zip file after extraction
        os.remove(zip_path)

    except Exception as e:
        error_message = f"failed to download citibike data from {url}\n{str(e)}"
        raise DownloadError(error_message) from e


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 2:
        year = int(sys.argv[1])
        download_citibike_data(year)
    elif len(sys.argv) == 3:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
        download_citibike_data(year, month)
    else:
        print("usage (yearly): python src/download_data.py 2018")
        print("usage (monthly): python src/download_data.py 2025 12")
