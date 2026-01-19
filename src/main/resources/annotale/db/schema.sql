CREATE TABLE IF NOT EXISTS tale (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  name_short TEXT,
  name_suffix TEXT,
  is_pseudo INTEGER,
  strain_id INTEGER,
  accession_id INTEGER,
  dna_seq TEXT,
  protein_seq TEXT,
  start_pos INTEGER,
  end_pos INTEGER,
  strand INTEGER,
  is_new INTEGER,
  FOREIGN KEY (strain_id) REFERENCES strain(id) ON DELETE SET NULL,
  FOREIGN KEY (accession_id) REFERENCES accession(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS repeat (
  tale_id INTEGER NOT NULL,
  ordinal INTEGER NOT NULL,
  rvd TEXT,
  rvd_pos INTEGER,
  rvd_len INTEGER,
  masked_seq_1 TEXT,
  masked_seq_2 TEXT,
  PRIMARY KEY (tale_id, ordinal),
  FOREIGN KEY (tale_id) REFERENCES tale(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_repeat_tale ON repeat(tale_id, ordinal);

CREATE TABLE IF NOT EXISTS analysis_config (
  id TEXT PRIMARY KEY,
  alignment_type TEXT,
  cut REAL,
  extra_gap_open REAL,
  extra_gap_ext REAL,
  linkage TEXT,
  pval REAL,
  cost_affine_open REAL,
  cost_affine_extend REAL,
  cost_rvd_gap REAL,
  cost_rvd_twelve REAL,
  cost_rvd_thirteen REAL,
  cost_rvd_bonus REAL,
  reserved_names TEXT
);

CREATE TABLE IF NOT EXISTS dmat (
  config_id TEXT PRIMARY KEY,
  data TEXT,
  FOREIGN KEY (config_id) REFERENCES analysis_config(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dmat_tale_order (
  config_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  tale_name TEXT NOT NULL,
  PRIMARY KEY (config_id, ordinal),
  FOREIGN KEY (config_id) REFERENCES analysis_config(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS family (
  name TEXT PRIMARY KEY,
  member_count INTEGER,
  tree_newick TEXT
);

CREATE TABLE IF NOT EXISTS family_member (
  family_id TEXT NOT NULL,
  tale_id INTEGER NOT NULL,
  PRIMARY KEY (family_id, tale_id),
  FOREIGN KEY (family_id) REFERENCES family(name) ON DELETE CASCADE,
  FOREIGN KEY (tale_id) REFERENCES tale(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS strain (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  species TEXT,
  pathovar TEXT,
  isolate TEXT,
  geo_tag TEXT,
  tax_id INTEGER
);

CREATE TABLE IF NOT EXISTS accession (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  replicon_type TEXT,
  strain_id INTEGER,
  UNIQUE(name, version),
  FOREIGN KEY (strain_id) REFERENCES strain(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_accession_name ON accession(name, version);

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS metadata;
