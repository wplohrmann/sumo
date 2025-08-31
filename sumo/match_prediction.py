import sqlite3
from collections import defaultdict
from typing import DefaultDict
from dataclasses import dataclass
import xgboost as xgb
from sklearn.metrics import accuracy_score
import numpy as np
from abc import ABC, abstractmethod
from tabulate import tabulate


DB_PATH = "sumo/sumo.db"



@dataclass
class Match:
    id: str
    basho_id: int
    rikishi1_id: int
    rikishi2_id: int
    winner_id: int
    day: int
    rikishi1_height: int
    rikishi1_weight: int
    rikishi2_height: int
    rikishi2_weight: int



def load_matches_and_basho_dates(db_path: str) -> tuple[list[Match], dict[int, str]]:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Get basho dates
    c.execute("SELECT id, start_date FROM basho")
    basho_dates = {row[0]: row[1] for row in c.fetchall()}
    # Get matches with height/weight for each rikishi in that basho
    c.execute(
        """
        SELECT m.id, m.basho_id, m.rikishi1_id, m.rikishi2_id, m.winner_id, m.day,
               m1.height_cm, m1.weight_kg, m2.height_cm, m2.weight_kg
        FROM match m
        LEFT JOIN measurement m1 ON m.rikishi1_id = m1.rikishi_id AND m.basho_id = m1.basho_id
        LEFT JOIN measurement m2 ON m.rikishi2_id = m2.rikishi_id AND m.basho_id = m2.basho_id
        """
    )
    matches = sorted(
        [Match(*row) for row in c.fetchall()],
        key=lambda m: (basho_dates[m.basho_id], m.day),
    )
    conn.close()
    return matches, basho_dates


def train_test_split(
    matches: list[Match], basho_dates: dict[int, str], split_date: str
) -> tuple[list[Match], list[Match]]:
    train, test = [], []
    for m in matches:
        date = basho_dates[m.basho_id]
        if date < split_date:
            train.append(m)
        else:
            test.append(m)
    return train, test


class BaseModel(ABC):
    @abstractmethod
    def fit(self, matches: list[Match]) -> float:
        pass

    @abstractmethod
    def evaluate(self, matches: list[Match]) -> float:
        pass

    @abstractmethod
    def name(self) -> str:
        pass


class EloModel(BaseModel):
    def __init__(self, K: float):
        self.stats: DefaultDict[int, float] = defaultdict(lambda: 1500)
        self.K = K

    def fit(self, matches: list[Match]) -> float:
        return self.evaluate(matches)

    def predict(self, rikishi1: int, rikishi2: int) -> int:
        mean1 = self.stats[rikishi1]
        mean2 = self.stats[rikishi2]
        return rikishi1 if mean1 > mean2 else rikishi2

    def update(self, rikishi1: int, rikishi2: int, winner: int) -> None:
        mean1 = self.stats[rikishi1]
        mean2 = self.stats[rikishi2]

        exp1 = 1 / (1 + 10 ** ((mean2 - mean1) / 400))
        exp2 = 1 - exp1
        s1 = 1 if winner == rikishi1 else 0
        s2 = 1 - s1

        new_mean1 = mean1 + self.K * (s1 - exp1)
        new_mean2 = mean2 + self.K * (s2 - exp2)
        self.stats[rikishi1] = new_mean1
        self.stats[rikishi2] = new_mean2

    def evaluate(self, matches: list[Match]) -> float:
        # Use a fresh model for evaluation
        correct = 0
        for m in sort_matches(matches):
            pred = self.predict(m.rikishi1_id, m.rikishi2_id)
            if pred == m.winner_id:
                correct += 1
            self.update(m.rikishi1_id, m.rikishi2_id, m.winner_id)
        return correct / len(matches) if matches else 0

    def name(self) -> str:
        return f"Elo({self.K})"


class XGBoostModel(BaseModel):
    def __init__(self):
        self.model = xgb.XGBClassifier(eval_metric="logloss")

    def fit(self, matches: list[Match]) -> float:
        X, y = extract_features(matches)
        self.model.fit(X, y)
        y_pred = self.model.predict(X)
        acc = float(accuracy_score(y, y_pred))
        return acc

    def evaluate(self, matches: list[Match]) -> float:
        X, y = extract_features(matches)
        y_pred = self.model.predict(X)
        acc = float(accuracy_score(y, y_pred))
        return acc

    def name(self) -> str:
        return "XGBoost"


def sort_matches(matches: list[Match]) -> list[Match]:
    return sorted(matches, key=lambda x: (x.basho_id, x.day))



def extract_features(matches: list[Match]) -> tuple[np.ndarray, np.ndarray]:
    # Features: rikishi1_id, rikishi2_id, rikishi1_height, rikishi1_weight, rikishi2_height, rikishi2_weight
    X = np.array([
        [
            m.rikishi1_id,
            m.rikishi2_id,
            m.rikishi1_height if m.rikishi1_height is not None else 0,
            m.rikishi1_weight if m.rikishi1_weight is not None else 0,
            m.rikishi2_height if m.rikishi2_height is not None else 0,
            m.rikishi2_weight if m.rikishi2_weight is not None else 0,
        ]
        for m in matches
    ])
    y = np.array([m.winner_id == m.rikishi1_id for m in matches], dtype=int)
    return X, y


if __name__ == "__main__":
    matches, basho_dates = load_matches_and_basho_dates(DB_PATH)
    split_date = "2023-01-01"
    train, test = train_test_split(matches, basho_dates, split_date)

    models: list[BaseModel] = [
        EloModel(K=8),
        EloModel(K=16),
        EloModel(K=32),
        EloModel(K=64),
        EloModel(K=128),
        XGBoostModel(),
    ]
    accs = {}
    for model in models:
        train_accuracy = model.fit(train)
        test_accuracy = model.evaluate(test)
        accs[model.name()] = (train_accuracy, test_accuracy)

    print(f"Train/test split: {len(train)}/{len(test)} matches")
    rows = [
        [name, f"{train_acc:.3f}", f"{test_acc:.3f}"]
        for name, (train_acc, test_acc) in sorted(accs.items(), key=lambda x: x[1][1], reverse=True)
    ]
    print(tabulate(rows, headers=["Model", "Train Accuracy", "Test Accuracy"], tablefmt="github"))
