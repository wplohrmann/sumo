CREATE TABLE basho (
    id INTEGER PRIMARY KEY,
    name TEXT,
    start_date DATE,
    end_date DATE
);

CREATE TABLE rikishi (
    id INTEGER PRIMARY KEY,
    name TEXT,
    rank TEXT,
    debut_date DATE,
    birth_date DATE
);

CREATE TABLE measurement (
    id INTEGER PRIMARY KEY,
    rikishi_id INTEGER,
    basho_id INTEGER,
    height_cm INTEGER,
    weight_kg INTEGER,
    FOREIGN KEY (rikishi_id) REFERENCES rikishi(id),
    FOREIGN KEY (basho_id) REFERENCES basho(id)
);

CREATE TABLE basho_rikishi (
    basho_id INTEGER,
    rikishi_id INTEGER,
    rank TEXT,
    division TEXT,
    PRIMARY KEY (basho_id, rikishi_id),
    FOREIGN KEY (basho_id) REFERENCES basho(id),
    FOREIGN KEY (rikishi_id) REFERENCES rikishi(id)
);

-- Table for Matches
CREATE TABLE match (
    id INTEGER PRIMARY KEY,
    basho_id INTEGER,
    rikishi1_id INTEGER,
    rikishi2_id INTEGER,
    winner_id INTEGER,
    kimarite TEXT,
    day INTEGER,
    match_date DATE,
    FOREIGN KEY (basho_id) REFERENCES basho(id),
    FOREIGN KEY (rikishi1_id) REFERENCES rikishi(id),
    FOREIGN KEY (rikishi2_id) REFERENCES rikishi(id),
    FOREIGN KEY (winner_id) REFERENCES rikishi(id)
);
