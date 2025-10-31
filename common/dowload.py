import requests


def download_file(url, output_path, headers=None, cookies=None):
    """Download file from URL to specified path."""
    try:
        response = requests.get(url, headers=headers, cookies=cookies, stream=True)
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False
