// Run once after deployment: node lib/migrate.js
// Requires DATABASE_URL env var (from Neon dashboard or .env.local)

const { neon } = require('@neondatabase/serverless')

async function migrate() {
  const sql = neon(process.env.DATABASE_URL)
  console.log('Running migrations...')

  await sql`
    CREATE TABLE IF NOT EXISTS dispatch_snapshots (
      id          SERIAL PRIMARY KEY,
      import_date DATE NOT NULL UNIQUE,
      import_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      source      TEXT NOT NULL DEFAULT 'manual',
      row_count   INTEGER NOT NULL,
      raw_json    JSONB NOT NULL
    )
  `
  await sql`CREATE INDEX IF NOT EXISTS idx_snapshots_date ON dispatch_snapshots (import_date DESC)`

  await sql`
    CREATE TABLE IF NOT EXISTS driver_scores (
      id               SERIAL PRIMARY KEY,
      score_date       DATE NOT NULL,
      driver_id        TEXT NOT NULL,
      driver_name      TEXT,
      final_score      NUMERIC(5,2),
      completion       NUMERIC(5,2),
      vol_score        NUMERIC(5,2),
      rej_score        NUMERIC(5,2),
      consistency      NUMERIC(5,2),
      avg_comp_rate    NUMERIC(5,2),
      avg_rej_rate     NUMERIC(5,2),
      days_active      INTEGER,
      total_drops      INTEGER,
      total_successful INTEGER,
      total_volume     NUMERIC,
      tier             TEXT,
      trend            TEXT,
      UNIQUE (score_date, driver_id)
    )
  `
  await sql`CREATE INDEX IF NOT EXISTS idx_driver_scores_date ON driver_scores (score_date DESC)`

  await sql`
    CREATE TABLE IF NOT EXISTS route_patterns (
      id           SERIAL PRIMARY KEY,
      pattern_date DATE NOT NULL,
      route_name   TEXT NOT NULL,
      sites        TEXT,
      zone         TEXT,
      team         TEXT,
      occurrences  INTEGER,
      avg_volume   NUMERIC,
      std_volume   NUMERIC,
      avg_drops    NUMERIC,
      avg_helpers  NUMERIC,
      rej_pct      NUMERIC,
      confidence   INTEGER,
      top_driver   TEXT,
      UNIQUE (pattern_date, route_name)
    )
  `
  await sql`CREATE INDEX IF NOT EXISTS idx_patterns_date ON route_patterns (pattern_date DESC)`

  console.log('✓ All tables created')
  process.exit(0)
}

migrate().catch(err => { console.error('Migration failed:', err); process.exit(1) })
