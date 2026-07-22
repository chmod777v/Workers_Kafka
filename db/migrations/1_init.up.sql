CREATE TABLE IF NOT EXISTS tasks
(
    id          SERIAL PRIMARY KEY,
    token       TEXT NOT NULL UNIQUE,
    message     TEXT
);