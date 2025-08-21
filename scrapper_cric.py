import requests
import zipfile
import io
import os
import json

def download_and_extract_data(url, output_dir="cricket_data_json"):
    print(f"Attempting to download data from: {url}")
    
    try:
        # Send a GET request to the URL to download the content
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        print("Download successful. Starting extraction...")

        # Use io.BytesIO to treat the downloaded content as a file in memory
        zip_file_in_memory = io.BytesIO(response.content)

        # Extract the contents of the zip file
        with zipfile.ZipFile(zip_file_in_memory, 'r') as zip_ref:
            # Create the output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            zip_ref.extractall(output_dir)

        print(f"Extraction complete. Files are located in the '{output_dir}' directory.")

    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}")
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid zip file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def load_and_print_first_file(directory):
    print("\nAttempting to read a sample JSON file for reference...")
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]
    
    if json_files:
        first_file_path = os.path.join(directory, json_files[0])
        try:
            with open(first_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"Successfully loaded '{os.path.basename(first_file_path)}'.")
                print("First 5 lines of the JSON data:")
                # Print a snippet of the data to verify it works
                for key, value in list(data.items())[:5]:
                    print(f"  {key}: {value}")
        except Exception as e:
            print(f"Error reading {first_file_path}: {e}")
    else:
        print("No JSON files found in the directory.")

if __name__ == "__main__":
    # The URL provided, which points to a zip file.
    DOWNLOAD_URL = "https://cricsheet.org/downloads/all_json.zip"
    
    # Execute the download and extraction process
    download_and_extract_data(DOWNLOAD_URL)
    
    # Verify the process by loading and printing a sample file
    load_and_print_first_file("cricket_data_json")
    
    print("\nScript finished.")