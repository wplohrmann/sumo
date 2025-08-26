-- Table for Basho (tournaments)
CREATE TABLE basho (
    id INTEGER PRIMARY KEY,
    name TEXT,
    start_date DATE,
    end_date DATE
);

-- Table for Rikishi (wrestlers)

CREATE TABLE rikishi (
    id INTEGER PRIMARY KEY,
    name TEXT,
    rank TEXT,
    debut_date DATE,
    birth_date DATE
);

-- Table for Rikishi measurements (height, weight per basho)
CREATE TABLE measurement (
    id INTEGER PRIMARY KEY,
    rikishi_id INTEGER,
    basho_id INTEGER,
    height_cm INTEGER,
    weight_kg INTEGER,
    FOREIGN KEY (rikishi_id) REFERENCES rikishi(id),
    FOREIGN KEY (basho_id) REFERENCES basho(id)
);

-- Table for Basho participation (which rikishi participated in which basho, with their rank for that basho)
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
    match_date DATE,
    FOREIGN KEY (basho_id) REFERENCES basho(id),
    FOREIGN KEY (rikishi1_id) REFERENCES rikishi(id),
    FOREIGN KEY (rikishi2_id) REFERENCES rikishi(id),
    FOREIGN KEY (winner_id) REFERENCES rikishi(id)
);
