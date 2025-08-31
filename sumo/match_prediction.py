# Elo-type model with mean/variance distributions for rikishi
# Train-test split by basho date, prediction, and evaluation
import sqlite3
import numpy as np
from collections import defaultdict
from typing import List, Tuple, Dict, DefaultDict

DB_PATH = "sumo/sumo.db"

from typing import List, Tuple, Dict

def load_matches_and_basho_dates(db_path: str) -> Tuple[List[Tuple[str, int, int, int, int, int]], Dict[int, str]]:
	conn = sqlite3.connect(db_path)
	c = conn.cursor()
	# Get basho dates
	c.execute("SELECT id, start_date FROM basho")
	basho_dates = {row[0]: row[1] for row in c.fetchall()}
	# Get matches
	c.execute("SELECT id, basho_id, rikishi1_id, rikishi2_id, winner_id, day FROM match ORDER BY match_date, day")
	matches = c.fetchall()
	conn.close()
	return matches, basho_dates

def train_test_split(
	matches: List[Tuple[str, int, int, int, int, int]],
	basho_dates: Dict[int, str],
	split_date: str
) -> Tuple[List[Tuple[str, int, int, int, int, int]], List[Tuple[str, int, int, int, int, int]]]:
	train, test = [], []
	for m in matches:
		basho_id = m[1]
		date = basho_dates[basho_id]
		if date < split_date:
			train.append(m)
		else:
			test.append(m)
	return train, test

class EloModel:
	stats: DefaultDict[int, float]
	K: int

	def __init__(self, initial_mean: float = 1500):
		self.stats: DefaultDict[int, float] = defaultdict(lambda: initial_mean)
		self.K = 32

	def predict(self, rikishi1: int, rikishi2: int) -> int:
		mean1 = self.stats[rikishi1]
		mean2 = self.stats[rikishi2]
		return rikishi1 if mean1 > mean2 else rikishi2

	def update(self, rikishi1: int, rikishi2: int, winner: int) -> None:
		mean1 = self.stats[rikishi1]
		mean2 = self.stats[rikishi2]
		# Expected score
		exp1 = 1 / (1 + 10 ** ((mean2 - mean1) / 400))
		exp2 = 1 - exp1
		# Actual score
		s1 = 1 if winner == rikishi1 else 0
		s2 = 1 - s1
		# Update mean
		new_mean1 = mean1 + self.K * (s1 - exp1)
		new_mean2 = mean2 + self.K * (s2 - exp2)
		self.stats[rikishi1] = new_mean1
		self.stats[rikishi2] = new_mean2

def evaluate(model, matches: List[Tuple[str, int, int, int, int, int]]) -> float:
    correct = 0
    for m in matches:
        _, _, rikishi1, rikishi2, winner, _ = m
        pred = model.predict(rikishi1, rikishi2)
        if pred == winner:
            correct += 1
        # Update Elo after prediction
        model.update(rikishi1, rikishi2, winner)
    return correct / len(matches) if matches else 0

def main() -> None:
	matches, basho_dates = load_matches_and_basho_dates(DB_PATH)
	# Choose split date (e.g., '2023-01-01')
	split_date = '2023-01-01'
	train, test = train_test_split(matches, basho_dates, split_date)
	model = EloModel()
	# Train
	for m in train:
		_, _, rikishi1, rikishi2, winner, _ = m
		model.update(rikishi1, rikishi2, winner)
	# Evaluate
	acc = evaluate(model, test)
	print(f"Test accuracy: {acc:.3f} ({len(test)} matches)")

if __name__ == "__main__":
	main()
