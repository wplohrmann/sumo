import json
import os
from typing import Any, Dict
import requests

BASE_URL = "https://www.sumo-api.com/api"
CACHE_PATH = ".cache"


def fetch(path: str) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    cache_path = os.path.join(CACHE_PATH, path.removeprefix("/") + ".json")
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            return json.load(f)
    response = requests.get(url)
    if response.status_code == 200:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(response.json(), f)
        return response.json()
    else:
        raise ValueError(f"Failed to fetch data in URL: {url}")
