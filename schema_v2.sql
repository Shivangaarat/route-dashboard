-- ============================================================
-- ROUTE PATTERN INTELLIGENCE DASHBOARD
-- Complete Database Schema v2.0
-- Run this entire file in Neon SQL Editor
-- ============================================================


-- ============================================================
-- TABLE 1: raw_tasks
-- Every individual task line from every daily upload
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_tasks (
    id                      SERIAL PRIMARY KEY,
    task_id                 TEXT NOT NULL,
    tour_id                 TEXT,
    planned_tour_name       TEXT,
    dispatch_date           DATE NOT NULL,
    team                    TEXT,
    temperature             TEXT,
    temp_category           TEXT,
    zone                    TEXT,
    city                    TEXT,
    rider_id                TEXT,
    rider_name              TEXT,
    vehicle_id              TEXT,
    vehicle_name            TEXT,
    vehicle_reg             TEXT,
    location_id             TEXT,
    location_name           TEXT,
    customer_name           TEXT,
    volume_cbm              NUMERIC(10,6),
    weight_kg               NUMERIC(10,3),
    task_status             TEXT,
    is_completed            BOOLEAN,
    is_failed               BOOLEAN,
    root_cause              TEXT,
    organisation            TEXT,
    division                TEXT,
    internal_org            TEXT,
    category                TEXT,
    invoice_value           NUMERIC(12,2),
    upload_ts               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    upload_source           TEXT DEFAULT 'manual'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_tasks_unique   ON raw_tasks (task_id, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_raw_tasks_date             ON raw_tasks (dispatch_date DESC);
CREATE INDEX IF NOT EXISTS idx_raw_tasks_tour             ON raw_tasks (planned_tour_name, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_raw_tasks_task             ON raw_tasks (task_id);
CREATE INDEX IF NOT EXISTS idx_raw_tasks_team             ON raw_tasks (team, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_raw_tasks_temp             ON raw_tasks (temp_category, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_raw_tasks_vehicle          ON raw_tasks (vehicle_id, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_raw_tasks_city             ON raw_tasks (city, dispatch_date);


-- ============================================================
-- TABLE 2: vehicle_master
-- Current active vehicle master
-- ============================================================
CREATE TABLE IF NOT EXISTS vehicle_master (
    vehicle_id              TEXT PRIMARY KEY,
    vehicle_name            TEXT,
    vehicle_model_id        TEXT,
    vehicle_model_name      TEXT,
    fleet_type              TEXT,
    ownership               TEXT,
    team_name               TEXT,
    vehicle_temp            TEXT,
    vehicle_type            TEXT,
    cbm_capacity            NUMERIC(8,3),
    model_cbm_capacity      NUMERIC(8,3),
    effective_cbm           NUMERIC(8,3),
    is_virtual              BOOLEAN DEFAULT FALSE,
    is_active               BOOLEAN DEFAULT TRUE,
    vehicle_reg             TEXT,
    upload_version_id       INTEGER,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vehicle_master_fleet       ON vehicle_master (fleet_type);
CREATE INDEX IF NOT EXISTS idx_vehicle_master_team        ON vehicle_master (team_name);
CREATE INDEX IF NOT EXISTS idx_vehicle_master_ownership   ON vehicle_master (ownership);


-- ============================================================
-- TABLE 3: vehicle_master_versions
-- Archive of every vehicle master upload
-- ============================================================
CREATE TABLE IF NOT EXISTS vehicle_master_versions (
    id                      SERIAL PRIMARY KEY,
    uploaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    uploaded_by             TEXT DEFAULT 'manual',
    vehicle_count           INTEGER,
    model_count             INTEGER,
    notes                   TEXT,
    vehicle_json            JSONB,
    model_json              JSONB,
    is_active               BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_vm_versions_active         ON vehicle_master_versions (is_active, uploaded_at DESC);


-- ============================================================
-- TABLE 4: route_exclusions
-- Configurable exclusion patterns from Settings tab
-- ============================================================
CREATE TABLE IF NOT EXISTS route_exclusions (
    id                      SERIAL PRIMARY KEY,
    pattern                 TEXT NOT NULL UNIQUE,
    match_type              TEXT DEFAULT 'prefix',
    reason                  TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    is_active               BOOLEAN DEFAULT TRUE
);

INSERT INTO route_exclusions (pattern, match_type, reason) VALUES
    ('SELF_DIC',    'prefix',  'Self-collection route'),
    ('SELF_HC',     'prefix',  'Self-collection HC route'),
    ('Self_route',  'exact',   'Virtual self route'),
    ('SELF_',       'prefix',  'All self-collection routes')
ON CONFLICT (pattern) DO NOTHING;


-- ============================================================
-- TABLE 5: daily_tour_metrics
-- Pre-calculated per tour per day
-- ============================================================
CREATE TABLE IF NOT EXISTS daily_tour_metrics (
    id                      SERIAL PRIMARY KEY,
    dispatch_date           DATE NOT NULL,
    planned_tour_name       TEXT NOT NULL,
    team                    TEXT,
    temp_category           TEXT,
    analysis_category       TEXT,
    city                    TEXT,
    zone                    TEXT,
    vehicle_id              TEXT,
    vehicle_name            TEXT,
    ownership               TEXT,
    fleet_type              TEXT,
    cbm_capacity            NUMERIC(8,3),
    is_virtual_vehicle      BOOLEAN DEFAULT FALSE,
    total_orders            INTEGER DEFAULT 0,
    unique_drops            INTEGER DEFAULT 0,
    completed_orders        INTEGER DEFAULT 0,
    failed_orders           INTEGER DEFAULT 0,
    total_volume_cbm        NUMERIC(10,6) DEFAULT 0,
    total_weight_kg         NUMERIC(10,3) DEFAULT 0,
    volume_util_pct         NUMERIC(6,2),
    pallet_util_pct         NUMERIC(6,2),
    route_type              TEXT,
    is_bulk                 BOOLEAN DEFAULT FALSE,
    is_excluded             BOOLEAN DEFAULT FALSE,
    rejection_pct           NUMERIC(6,2),
    pharma_orders           INTEGER DEFAULT 0,
    medlab_orders           INTEGER DEFAULT 0,
    pharma_failed           INTEGER DEFAULT 0,
    medlab_failed           INTEGER DEFAULT 0,
    rd_orders               INTEGER DEFAULT 0,
    rd_pct                  NUMERIC(6,2),
    calculated_at           TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (dispatch_date, planned_tour_name)
);

CREATE INDEX IF NOT EXISTS idx_dtm_date       ON daily_tour_metrics (dispatch_date DESC);
CREATE INDEX IF NOT EXISTS idx_dtm_team       ON daily_tour_metrics (team, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_dtm_category   ON daily_tour_metrics (analysis_category, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_dtm_ownership  ON daily_tour_metrics (ownership, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_dtm_bulk       ON daily_tour_metrics (is_bulk, dispatch_date);
CREATE INDEX IF NOT EXISTS idx_dtm_excluded   ON daily_tour_metrics (is_excluded);


-- ============================================================
-- TABLE 6: daily_summary
-- Aggregated daily KPIs per analysis category
-- ============================================================
CREATE TABLE IF NOT EXISTS daily_summary (
    id                          SERIAL PRIMARY KEY,
    dispatch_date               DATE NOT NULL,
    analysis_category           TEXT NOT NULL,

    total_tours                 INTEGER DEFAULT 0,
    excluded_tours              INTEGER DEFAULT 0,
    included_tours              INTEGER DEFAULT 0,
    own_vehicles                INTEGER DEFAULT 0,
    dleased_vehicles            INTEGER DEFAULT 0,
    total_drops                 INTEGER DEFAULT 0,
    own_drops                   INTEGER DEFAULT 0,
    dleased_drops               INTEGER DEFAULT 0,
    own_avg_drops               NUMERIC(6,2),
    dleased_avg_drops           NUMERIC(6,2),
    overall_avg_drops           NUMERIC(6,2),
    avg_drops_excl_single       NUMERIC(6,2),
    single_drop_count           INTEGER DEFAULT 0,
    multi_drop_vehicle_count    INTEGER DEFAULT 0,
    multi_drop_total            INTEGER DEFAULT 0,
    single_drop_vehicle_count   INTEGER DEFAULT 0,
    single_drop_total           INTEGER DEFAULT 0,
    total_volume_cbm            NUMERIC(10,3) DEFAULT 0,
    total_weight_kg             NUMERIC(12,3) DEFAULT 0,
    avg_volume_util_pct         NUMERIC(6,2),
    avg_pallet_util_pct         NUMERIC(6,2),
    bulk_route_count            INTEGER DEFAULT 0,
    bulk_route_pct              NUMERIC(6,2),
    total_orders                INTEGER DEFAULT 0,
    completed_orders            INTEGER DEFAULT 0,
    failed_orders               INTEGER DEFAULT 0,
    daily_rejection_pct         NUMERIC(6,2),
    first_attempt_success_pct   NUMERIC(6,2),
    rd_count                    INTEGER DEFAULT 0,
    rd_pct                      NUMERIC(6,2),
    rd_pharma_pct               NUMERIC(6,2),
    rd_medlab_pct               NUMERIC(6,2),
    calculated_at               TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (dispatch_date, analysis_category)
);

CREATE INDEX IF NOT EXISTS idx_ds_date      ON daily_summary (dispatch_date DESC);
CREATE INDEX IF NOT EXISTS idx_ds_category  ON daily_summary (analysis_category, dispatch_date DESC);


-- ============================================================
-- TABLE 7: emirates_daily
-- City breakdown per day per category
-- ============================================================
CREATE TABLE IF NOT EXISTS emirates_daily (
    id                  SERIAL PRIMARY KEY,
    dispatch_date       DATE NOT NULL,
    analysis_category   TEXT NOT NULL,
    city                TEXT NOT NULL,
    total_orders        INTEGER DEFAULT 0,
    total_drops         INTEGER DEFAULT 0,
    completed_orders    INTEGER DEFAULT 0,
    failed_orders       INTEGER DEFAULT 0,
    total_volume_cbm    NUMERIC(10,3) DEFAULT 0,
    rejection_pct       NUMERIC(6,2),
    calculated_at       TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (dispatch_date, analysis_category, city)
);

CREATE INDEX IF NOT EXISTS idx_em_date      ON emirates_daily (dispatch_date DESC);
CREATE INDEX IF NOT EXISTS idx_em_category  ON emirates_daily (analysis_category, dispatch_date DESC);
CREATE INDEX IF NOT EXISTS idx_em_city      ON emirates_daily (city, dispatch_date DESC);


-- ============================================================
-- TABLE 8: task_attempt_history
-- Re-delivery tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS task_attempt_history (
    id                  SERIAL PRIMARY KEY,
    task_id             TEXT NOT NULL,
    attempt_number      INTEGER NOT NULL,
    attempt_date        DATE NOT NULL,
    status              TEXT,
    root_cause          TEXT,
    rider_id            TEXT,
    vehicle_id          TEXT,
    planned_tour_name   TEXT,
    is_final_success    BOOLEAN DEFAULT FALSE,
    days_since_first    INTEGER,

    UNIQUE (task_id, attempt_date)
);

CREATE INDEX IF NOT EXISTS idx_tah_task     ON task_attempt_history (task_id);
CREATE INDEX IF NOT EXISTS idx_tah_date     ON task_attempt_history (attempt_date DESC);
CREATE INDEX IF NOT EXISTS idx_tah_success  ON task_attempt_history (is_final_success, attempt_date DESC);


-- ============================================================
-- TABLE 9: settings
-- ============================================================
CREATE TABLE IF NOT EXISTS settings (
    key                 TEXT PRIMARY KEY,
    value               TEXT,
    description         TEXT,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO settings (key, value, description) VALUES
    ('bulk_min_util_pct',     '80',        'Minimum CBM util % for bulk route'),
    ('bulk_min_drops',        '3',         'Minimum drops for multi-drop route'),
    ('single_drop_max',       '2',         'Maximum drops for single drop route'),
    ('pharma_patterns',       'Medicine',  'CATEGORY values for Pharma (comma-separated)'),
    ('medlab_patterns',       'Medlab',    'CATEGORY values for Medlab (comma-separated)'),
    ('rd_denominator',        'total',     'RD% denominator: total or failed'),
    ('auto_backup_enabled',   'true',      'Weekly auto-backup toggle'),
    ('backup_day',            'Sunday',    'Day of week for auto-backup'),
    ('keepalive_enabled',     'true',      'Weekend keepalive cron toggle'),
    ('dashboard_title',       'Route Pattern Intelligence', 'Dashboard header title')
ON CONFLICT (key) DO NOTHING;


-- ============================================================
-- TABLE 10: database_backups
-- ============================================================
CREATE TABLE IF NOT EXISTS database_backups (
    id                  SERIAL PRIMARY KEY,
    backup_type         TEXT NOT NULL,
    triggered_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              TEXT DEFAULT 'pending',
    raw_task_count      INTEGER,
    tour_metric_count   INTEGER,
    summary_count       INTEGER,
    file_size_kb        NUMERIC(10,2),
    error_message       TEXT,
    triggered_by        TEXT DEFAULT 'system'
);

CREATE INDEX IF NOT EXISTS idx_backups_date ON database_backups (triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_backups_type ON database_backups (backup_type, triggered_at DESC);


-- ============================================================
-- VIEWS
-- ============================================================

CREATE OR REPLACE VIEW mtd_summary AS
SELECT
    analysis_category,
    DATE_TRUNC('month', dispatch_date)::DATE        AS month_start,
    TO_CHAR(dispatch_date, 'Mon-YY')               AS month_label,
    COUNT(DISTINCT dispatch_date)                  AS days_active,
    SUM(included_tours)                            AS total_tours,
    SUM(total_drops)                               AS total_drops,
    SUM(own_vehicles)                              AS own_vehicles,
    SUM(own_drops)                                 AS own_drops,
    SUM(dleased_vehicles)                          AS dleased_vehicles,
    SUM(dleased_drops)                             AS dleased_drops,
    ROUND(AVG(own_avg_drops), 2)                   AS own_avg_drops,
    ROUND(AVG(dleased_avg_drops), 2)               AS dleased_avg_drops,
    ROUND(AVG(overall_avg_drops), 2)               AS overall_avg_drops,
    ROUND(AVG(avg_drops_excl_single), 2)           AS avg_drops_excl_single,
    SUM(single_drop_count)                         AS single_drop_count,
    SUM(multi_drop_vehicle_count)                  AS multi_veh_count,
    SUM(multi_drop_total)                          AS multi_drop_total,
    SUM(single_drop_vehicle_count)                 AS single_veh_count,
    SUM(single_drop_total)                         AS single_drop_total,
    ROUND(AVG(avg_volume_util_pct), 2)             AS avg_cbm_util_pct,
    ROUND(AVG(avg_pallet_util_pct), 2)             AS avg_pallet_util_pct,
    SUM(bulk_route_count)                          AS bulk_routes,
    SUM(total_orders)                              AS total_orders,
    SUM(completed_orders)                          AS completed_orders,
    SUM(failed_orders)                             AS failed_orders,
    ROUND(SUM(failed_orders)::NUMERIC / NULLIF(SUM(total_orders),0) * 100, 2) AS mtd_rejection_pct,
    ROUND(AVG(rd_pct), 2)                          AS mtd_rd_pct,
    ROUND(AVG(rd_pharma_pct), 2)                   AS mtd_rd_pharma_pct,
    ROUND(AVG(rd_medlab_pct), 2)                   AS mtd_rd_medlab_pct,
    ROUND(AVG(first_attempt_success_pct), 2)       AS first_attempt_success_pct
FROM daily_summary
GROUP BY analysis_category,
         DATE_TRUNC('month', dispatch_date),
         TO_CHAR(dispatch_date, 'Mon-YY')
ORDER BY month_start DESC, analysis_category;


CREATE OR REPLACE VIEW ytd_summary AS
SELECT
    analysis_category,
    DATE_TRUNC('month', dispatch_date)::DATE        AS month_start,
    TO_CHAR(dispatch_date, 'Mon-YY')               AS month_label,
    EXTRACT(YEAR FROM dispatch_date)               AS year,
    COUNT(DISTINCT dispatch_date)                  AS days_active,
    SUM(included_tours)                            AS total_tours,
    SUM(total_drops)                               AS total_drops,
    SUM(own_drops)                                 AS own_drops,
    SUM(dleased_drops)                             AS dleased_drops,
    ROUND(AVG(overall_avg_drops), 2)               AS overall_avg_drops,
    ROUND(AVG(avg_drops_excl_single), 2)           AS avg_drops_excl_single,
    SUM(single_drop_count)                         AS single_drop_count,
    ROUND(AVG(avg_volume_util_pct), 2)             AS avg_cbm_util_pct,
    ROUND(AVG(avg_pallet_util_pct), 2)             AS avg_pallet_util_pct,
    SUM(bulk_route_count)                          AS bulk_routes,
    SUM(total_orders)                              AS total_orders,
    ROUND(SUM(failed_orders)::NUMERIC / NULLIF(SUM(total_orders),0) * 100, 2) AS ytd_rejection_pct,
    ROUND(AVG(rd_pct), 2)                          AS ytd_rd_pct,
    ROUND(AVG(rd_pharma_pct), 2)                   AS ytd_rd_pharma_pct,
    ROUND(AVG(rd_medlab_pct), 2)                   AS ytd_rd_medlab_pct,
    ROUND(AVG(first_attempt_success_pct), 2)       AS first_attempt_success_pct
FROM daily_summary
WHERE EXTRACT(YEAR FROM dispatch_date) = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY analysis_category,
         DATE_TRUNC('month', dispatch_date),
         TO_CHAR(dispatch_date, 'Mon-YY'),
         EXTRACT(YEAR FROM dispatch_date)
ORDER BY month_start DESC, analysis_category;


CREATE OR REPLACE VIEW emirates_mtd AS
SELECT
    analysis_category,
    city,
    DATE_TRUNC('month', dispatch_date)::DATE        AS month_start,
    TO_CHAR(dispatch_date, 'Mon-YY')               AS month_label,
    SUM(total_orders)                              AS total_orders,
    SUM(total_drops)                               AS total_drops,
    SUM(completed_orders)                          AS completed_orders,
    SUM(failed_orders)                             AS failed_orders,
    SUM(total_volume_cbm)                          AS total_volume_cbm,
    ROUND(SUM(failed_orders)::NUMERIC / NULLIF(SUM(total_orders),0) * 100, 2) AS rejection_pct
FROM emirates_daily
GROUP BY analysis_category, city,
         DATE_TRUNC('month', dispatch_date),
         TO_CHAR(dispatch_date, 'Mon-YY')
ORDER BY month_start DESC, analysis_category, total_drops DESC;


CREATE OR REPLACE VIEW redelivery_summary AS
SELECT
    task_id,
    COUNT(*)                                        AS total_attempts,
    MIN(attempt_date)                              AS first_attempt_date,
    MAX(attempt_date)                              AS last_attempt_date,
    (MAX(attempt_date) - MIN(attempt_date))        AS days_to_deliver,
    MAX(CASE WHEN is_final_success THEN status END) AS final_status,
    BOOL_OR(is_final_success)                      AS was_delivered
FROM task_attempt_history
GROUP BY task_id
HAVING COUNT(*) > 1
ORDER BY total_attempts DESC;


-- ============================================================
-- VERIFICATION
-- ============================================================
SELECT
    'Schema v2.0 created successfully' AS status,
    (SELECT COUNT(*) FROM information_schema.tables
     WHERE table_schema = 'public'
     AND table_type = 'BASE TABLE')    AS tables,
    (SELECT COUNT(*) FROM information_schema.views
     WHERE table_schema = 'public')    AS views,
    (SELECT COUNT(*) FROM route_exclusions) AS default_exclusions,
    (SELECT COUNT(*) FROM settings)    AS default_settings;
