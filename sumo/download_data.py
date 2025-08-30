

import os
import sqlite3
import requests
from typing import List
import logging
from tqdm import tqdm

from sumo.bashos import bashos

DB_PATH = os.path.join(os.path.dirname(__file__), 'sumo.db')
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schema.sql')
BASE_URL = "https://www.sumo-api.com/api"


# Helper to initialize DB if not exists
def init_db():
	if not os.path.exists(DB_PATH):
		with open(SCHEMA_PATH) as f:
			schema = f.read()
		conn = sqlite3.connect(DB_PATH)
		conn.executescript(schema)
		conn.close()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Helper to insert basho data
def insert_basho(conn, basho):
	# Use bashoId as id
	basho_id = basho.get('date') or basho.get('id')
	conn.execute(
		"INSERT OR IGNORE INTO basho (id, name, start_date, end_date) VALUES (?, ?, ?, ?)",
		(basho_id, basho.get('location'), basho.get('startDate'), basho.get('endDate'))
	)

# Main function
def load_basho_data(basho_ids: List[str]):
	init_db()
	conn = sqlite3.connect(DB_PATH)
	divisions = ["Makuuchi", "Juryo", "Makushita", "Sandanme", "Jonidan", "Jonokuchi"]
	logging.info(f"Starting to process {len(basho_ids)} bashos...")
	for basho_id in tqdm(basho_ids, desc="Bashos"):
		# Check if basho already exists
		basho_exists = conn.execute("SELECT 1 FROM basho WHERE id = ?", (basho_id,)).fetchone()
		if not basho_exists:
			logging.info(f"Fetching basho {basho_id}")
			basho_resp = requests.get(f"{BASE_URL}/basho/{basho_id}")
			basho = basho_resp.json()
			insert_basho(conn, basho)
		else:
			logging.info(f"Basho {basho_id} already in DB")
			basho = {"date": basho_id}  # minimal info for downstream logic

	# Load rikishi and ranks from banzuke for each division
	rikishi_ids = set()
	for division in tqdm(divisions, desc=f"Divisions for {basho_id}", leave=False):
			# Only fetch banzuke if at least one rikishi for this basho/division is missing
			banzuke_needed = False
			for side in ["east", "west"]:
				# Check if any rikishi for this basho/division/side is missing
				row = conn.execute(
					"SELECT 1 FROM basho_rikishi WHERE basho_id = ? AND division = ? LIMIT 1", (basho_id, division)
				).fetchone()
				if not row:
					banzuke_needed = True
					break
			if banzuke_needed:
				logging.info(f"Fetching banzuke for basho {basho_id}, division {division}")
				banzuke_url = f"{BASE_URL}/basho/{basho_id}/banzuke/{division}"
				banzuke_resp = requests.get(banzuke_url)
				if banzuke_resp.status_code != 200:
					logging.warning(f"Failed to fetch banzuke for basho {basho_id}, division {division}")
					continue
				banzuke = banzuke_resp.json()
				for side in ["east", "west"]:
					for rikishi in banzuke.get(side, []):
						rikishi_id = rikishi.get("rikishiID")
						rikishi_ids.add(rikishi_id)
						conn.execute(
							"INSERT OR IGNORE INTO basho_rikishi (basho_id, rikishi_id, rank, division) VALUES (?, ?, ?, ?)",
							(basho_id, rikishi_id, rikishi.get("rank"), division)
						)

	# Load rikishi details
	logging.info(f"Processing {len(rikishi_ids)} rikishi for basho {basho_id}")
	for rikishi_id in tqdm(list(rikishi_ids), desc=f"Rikishi for {basho_id}", leave=False):
			# Only fetch rikishi if not in DB
			rikishi_exists = conn.execute("SELECT 1 FROM rikishi WHERE id = ?", (rikishi_id,)).fetchone()
			if not rikishi_exists:
				logging.info(f"Fetching rikishi {rikishi_id}")
				rikishi_url = f"{BASE_URL}/rikishi/{rikishi_id}"
				rikishi_resp = requests.get(rikishi_url)
				if rikishi_resp.status_code != 200:
					logging.warning(f"Failed to fetch rikishi {rikishi_id}")
					continue
				rikishi = rikishi_resp.json()
				conn.execute(
					"INSERT OR IGNORE INTO rikishi (id, name, rank, debut_date, birth_date) VALUES (?, ?, ?, ?, ?)",
					(
						rikishi.get("id"),
						rikishi.get("shikonaEn"),
						None,  # rank is handled in basho_rikishi
						rikishi.get("debut"),
						rikishi.get("birthDate")
					)
				)

		# Load measurements for this basho
		logging.info(f"Fetching measurements for basho {basho_id}")
		meas_url = f"{BASE_URL}/measurements?bashoId={basho_id}"
		meas_resp = requests.get(meas_url)
		if meas_resp.status_code == 200:
			measurements = meas_resp.json()
			for m in measurements:
				conn.execute(
					"INSERT OR IGNORE INTO measurement (rikishi_id, basho_id, height_cm, weight_kg) VALUES (?, ?, ?, ?)",
					(m.get("rikishiId"), basho_id, m.get("height"), m.get("weight"))
				)
		else:
			logging.warning(f"Failed to fetch measurements for basho {basho_id}")

		# Load matches for each division and day (1-15)
		for division in tqdm(divisions, desc=f"Match Divisions {basho_id}", leave=False):
			for day in tqdm(range(1, 16), desc=f"Days {division}", leave=False):
				# Only fetch matches if not present for this basho/division/day
				match_exists = conn.execute(
					"SELECT 1 FROM match WHERE basho_id = ? AND EXISTS (SELECT 1 FROM basho_rikishi WHERE basho_id = ? AND division = ?) LIMIT 1",
					(basho_id, basho_id, division)
				).fetchone()
				if not match_exists:
					logging.info(f"Fetching matches for basho {basho_id}, division {division}, day {day}")
					match_url = f"{BASE_URL}/basho/{basho_id}/torikumi/{division}/{day}"
					match_resp = requests.get(match_url)
					if match_resp.status_code != 200:
						logging.warning(f"Failed to fetch matches for basho {basho_id}, division {division}, day {day}")
						continue
					match_data = match_resp.json()
					for match in match_data.get("torikumi", []):
						conn.execute(
							"INSERT OR IGNORE INTO match (id, basho_id, rikishi1_id, rikishi2_id, winner_id, kimarite, match_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
							(
								match.get("id"),
								basho_id,
								match.get("eastId"),
								match.get("westId"),
								match.get("winnerId"),
								match.get("kimarite"),
								None  # match_date not provided in API response
							)
						)
	conn.commit()
	conn.close()

if __name__ == "__main__":
	basho_ids = bashos[:5]
	load_basho_data(basho_ids)
