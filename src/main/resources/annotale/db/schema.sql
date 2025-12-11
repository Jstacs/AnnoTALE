CREATE TABLE IF NOT EXISTS tale (
  id TEXT PRIMARY KEY,
  strain TEXT,
  accession TEXT,
  start_pos INTEGER,
  end_pos INTEGER,
  strand INTEGER,
  is_new INTEGER
);

CREATE TABLE IF NOT EXISTS repeat (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tale_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  rvd TEXT,
  rvd_pos INTEGER,
  rvd_len INTEGER,
  repeat_seq TEXT,
  masked_seq1 TEXT,
  masked_seq2 TEXT,
  FOREIGN KEY (tale_id) REFERENCES tale(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_repeat_tale ON repeat(tale_id, ordinal);

CREATE TABLE IF NOT EXISTS metadata (
  key TEXT PRIMARY KEY,
  value TEXT
);
