import requests


BASE_URL = "https://www.sumo-api.com/api"

r = requests.get(BASE_URL + "/rikishis")
r.json()["records"][0]
