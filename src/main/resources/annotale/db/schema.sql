CREATE TABLE IF NOT EXISTS taxonomy (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ncbi_tax_id INTEGER UNIQUE,
  rank TEXT,
  name TEXT NOT NULL,
  species TEXT,
  pathovar TEXT
);

CREATE TABLE IF NOT EXISTS samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  legacy_strain_name TEXT,
  strain_name TEXT,
  geo_tag TEXT,
  collection_date TEXT,
  biosample_id TEXT UNIQUE,
  taxon_id INTEGER NOT NULL,
  FOREIGN KEY (taxon_id) REFERENCES taxonomy(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS assembly (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  accession TEXT,
  version TEXT,
  replicon_type TEXT,
  sample_id INTEGER,
  FOREIGN KEY (sample_id) REFERENCES samples(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_assembly_accession
ON assembly(accession, version)
WHERE accession IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_assembly_local_per_sample
ON assembly(sample_id)
WHERE accession IS NULL;

CREATE TABLE IF NOT EXISTS tale (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  legacy_name TEXT NOT NULL UNIQUE,
  dna_seq TEXT,
  protein_seq TEXT,
  start_pos INTEGER,
  end_pos INTEGER,
  strand INTEGER,
  is_new INTEGER,
  is_pseudo INTEGER,
  assembly_id INTEGER,
  FOREIGN KEY (assembly_id) REFERENCES assembly(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS repeat (
  tale_id INTEGER NOT NULL,
  repeat_ordinal INTEGER NOT NULL,
  rvd TEXT,
  rvd_pos INTEGER,
  rvd_len INTEGER,
  masked_seq_1 TEXT,
  masked_seq_2 TEXT,
  PRIMARY KEY (tale_id, repeat_ordinal),
  FOREIGN KEY (tale_id) REFERENCES tale(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_repeat_tale ON repeat(tale_id, repeat_ordinal);

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
  id INTEGER PRIMARY KEY,
  data TEXT
);

CREATE TABLE IF NOT EXISTS dmat_tale_order (
  ordinal INTEGER NOT NULL,
  tale_id INTEGER NOT NULL,
  PRIMARY KEY (ordinal),
  FOREIGN KEY (tale_id) REFERENCES tale(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tale_family (
  name TEXT PRIMARY KEY,
  member_count INTEGER,
  tree_newick TEXT
);

CREATE TABLE IF NOT EXISTS tale_family_member (
  family_id TEXT NOT NULL,
  tale_id INTEGER NOT NULL,
  PRIMARY KEY (family_id, tale_id),
  FOREIGN KEY (family_id) REFERENCES tale_family(name) ON DELETE CASCADE,
  FOREIGN KEY (tale_id) REFERENCES tale(id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_version (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  version INTEGER NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS strain;
DROP TABLE IF EXISTS accession;
DROP TABLE IF EXISTS family;
DROP TABLE IF EXISTS family_member;
