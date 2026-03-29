// lib/db.js
import { neon } from '@neondatabase/serverless'
import { detectPatterns, computeDriverScores } from './engine.js'

function sql() {
  return neon(process.env.DATABASE_URL)
}

export async function saveSnapshot({ rows, source = 'manual', importDate }) {
  const db = sql()
  const date = importDate || new Date().toISOString().split('T')[0]
  const patterns = detectPatterns(rows)
  const driverScores = computeDriverScores(rows)

  await db`
    INSERT INTO dispatch_snapshots (import_date, source, row_count, raw_json)
    VALUES (${date}, ${source}, ${rows.length}, ${JSON.stringify(rows)}::jsonb)
    ON CONFLICT DO NOTHING
  `

  for (const d of driverScores) {
    await db`
      INSERT INTO driver_scores
        (score_date, driver_id, driver_name, final_score, completion, vol_score,
         rej_score, consistency, avg_comp_rate, avg_rej_rate, days_active,
         total_drops, total_successful, total_volume, tier, trend)
      VALUES
        (${date}, ${d.id}, ${d.name}, ${d.finalScore},
         ${d.scores.completion}, ${d.scores.volume}, ${d.scores.rejection}, ${d.scores.consistency},
         ${d.avgCompRate}, ${d.avgRejRate}, ${d.daysActive},
         ${d.totalDrops}, ${d.totalSuccessful}, ${d.totalVol}, ${d.tier}, ${d.trend})
      ON CONFLICT (score_date, driver_id)
      DO UPDATE SET
        final_score = EXCLUDED.final_score,
        completion  = EXCLUDED.completion,
        tier        = EXCLUDED.tier,
        trend       = EXCLUDED.trend
    `
  }

  for (const p of patterns) {
    await db`
      INSERT INTO route_patterns
        (pattern_date, route_name, sites, zone, team, occurrences,
         avg_volume, std_volume, avg_drops, avg_helpers, rej_pct, confidence, top_driver)
      VALUES
        (${date}, ${p.name || ''}, ${p.sites || ''}, ${p.zone || ''}, ${p.team || ''},
         ${p.occurrences}, ${p.avgVol}, ${p.stdVol}, ${p.avgDrops}, ${p.avgHelpers},
         ${p.rejPct}, ${p.confidence}, ${p.topDriver || ''})
      ON CONFLICT (pattern_date, route_name)
      DO UPDATE SET
        occurrences = EXCLUDED.occurrences,
        confidence  = EXCLUDED.confidence,
        rej_pct     = EXCLUDED.rej_pct
    `
  }

  return { date, rowCount: rows.length, patterns: patterns.length, drivers: driverScores.length }
}

export async function getAvailableDates() {
  const db = sql()
  const rows = await db`
    SELECT import_date::text, source, row_count
    FROM dispatch_snapshots
    ORDER BY import_date DESC
    LIMIT 90
  `
  return rows
}

export async function getSnapshot(date) {
  const db = sql()
  const rows = await db`
    SELECT raw_json, import_ts, source
    FROM dispatch_snapshots
    WHERE import_date = ${date}
    ORDER BY import_ts DESC
    LIMIT 1
  `
  if (!rows.length) return null
  return { data: rows[0].raw_json, importedAt: rows[0].import_ts, source: rows[0].source }
}

export async function getDriverHistory(driverId, days = 30) {
  const db = sql()
  const rows = await db`
    SELECT score_date::text, final_score, avg_comp_rate, avg_rej_rate, tier, trend
    FROM driver_scores
    WHERE driver_id = ${driverId}
    ORDER BY score_date DESC
    LIMIT ${days}
  `
  return rows
}

export async function getPatternHistory(routeName, days = 30) {
  const db = sql()
  const rows = await db`
    SELECT pattern_date::text, occurrences, avg_volume, rej_pct, confidence
    FROM route_patterns
    WHERE route_name = ${routeName}
    ORDER BY pattern_date DESC
    LIMIT ${days}
  `
  return rows
}
