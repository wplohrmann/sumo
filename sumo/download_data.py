import requests


BASE_URL = "https://www.sumo-api.com/api"

r = requests.get(BASE_URL + "/basho/202301")
r.json().keys()
