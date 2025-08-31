# Elo-type model with mean/variance distributions for rikishi
# Train-test split by basho date, prediction, and evaluation
import sqlite3
from collections import defaultdict
from typing import DefaultDict
from dataclasses import dataclass
import xgboost as xgb
from sklearn.metrics import accuracy_score
import numpy as np
from abc import ABC, abstractmethod


DB_PATH = "sumo/sumo.db"


@dataclass
class Match:
    id: str
    basho_id: int
    rikishi1_id: int
    rikishi2_id: int
    winner_id: int
    day: int


def load_matches_and_basho_dates(db_path: str) -> tuple[list[Match], dict[int, str]]:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Get basho dates
    c.execute("SELECT id, start_date FROM basho")
    basho_dates = {row[0]: row[1] for row in c.fetchall()}
    # Get matches
    c.execute(
        "SELECT id, basho_id, rikishi1_id, rikishi2_id, winner_id, day FROM match"
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
    def fit(self, matches: list[Match]):
        pass

    @abstractmethod
    def predict(self, rikishi1: int, rikishi2: int) -> int:
        pass

    @abstractmethod
    def evaluate(self, matches: list[Match]) -> float:
        pass

class EloModel(BaseModel):
    def __init__(self, K: float):
        self.stats: DefaultDict[int, float] = defaultdict(lambda: 1500)
        self.K = K

    def fit(self, matches: list[Match]):
        for m in sort_matches(matches):
            self.update(m.rikishi1_id, m.rikishi2_id, m.winner_id)

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

class XGBoostModel(BaseModel):
    def __init__(self):
        self.model = xgb.XGBClassifier(eval_metric='logloss')
        self.is_fitted = False

    def fit(self, matches: list[Match]):
        X, y = extract_features(matches)
        self.model.fit(X, y)
        self.is_fitted = True

    def predict(self, rikishi1: int, rikishi2: int) -> int:
        if not self.is_fitted:
            raise ValueError("Model not fitted.")
        X = np.array([[rikishi1, rikishi2]])
        pred = self.model.predict(X)[0]
        return rikishi1 if pred == 1 else rikishi2

    def evaluate(self, matches: list[Match]) -> float:
        X, y = extract_features(matches)
        y_pred = self.model.predict(X)
        acc = float(accuracy_score(y, y_pred))
        return acc


def sort_matches(matches: list[Match]) -> list[Match]:
    return sorted(matches, key=lambda x: (x.basho_id, x.day))




def extract_features(matches: list[Match]) -> tuple[np.ndarray, np.ndarray]:
    # Simple features: rikishi1_id, rikishi2_id
    X = np.array([[m.rikishi1_id, m.rikishi2_id] for m in matches])
    y = np.array([m.winner_id == m.rikishi1_id for m in matches], dtype=int)
    return X, y


def main() -> None:
    matches, basho_dates = load_matches_and_basho_dates(DB_PATH)
    split_date = "2023-01-01"
    train, test = train_test_split(matches, basho_dates, split_date)

    # Parameter grid
    K_values = [8, 16, 32, 64, 128, 256, 512]

    best_acc = -1
    best_K = None

    for K in K_values:
        model = EloModel(K=K)
        acc = model.evaluate(train)
        print(f"Params: K={K} => Train accuracy: {acc:.3f}")
        if acc > best_acc:
            best_acc = acc
            best_K = K

    assert best_K is not None
    print(f"Best K: {best_K} => Train accuracy: {best_acc:.3f}")
    final_model = EloModel(K=best_K)
    final_model.fit(train)
    final_acc = final_model.evaluate(test)
    print(f"Final evaluation with best K: Test accuracy: {final_acc:.3f} ({len(test)} matches)")

    xgb_model = XGBoostModel()
    xgb_model.fit(train)
    xgb_acc = xgb_model.evaluate(test)
    print(f"XGBoost Classifier Test accuracy: {xgb_acc:.3f} ({len(test)} matches)")

if __name__ == "__main__":
    main()
