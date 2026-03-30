// app/api/recalculate/route.js
// POST { date: '2026-03-19' } — runs KPI engine for one date

import { recalculateDay } from '../../../lib/kpi-engine.js'
import { neon } from '@neondatabase/serverless'

export const runtime = 'nodejs'
export const maxDuration = 60

function db() { return neon(process.env.DATABASE_URL) }

export async function POST(request) {
  try {
    const { date } = await request.json()
    if (!date) return Response.json({ error: 'date required' }, { status: 400 })

    const sql = db()

    // ── Update attempt history for this date ─────────────────────────────────
    // Find all tasks for this date and check if they appeared on earlier dates
    const tasksForDate = await sql`
      SELECT task_id, task_status, is_completed, is_failed, root_cause,
             rider_id, vehicle_id, planned_tour_name
      FROM raw_tasks
      WHERE dispatch_date = ${date}
      AND (is_completed = true OR is_failed = true)
    `

    for (const t of tasksForDate) {
      // Check if this task appeared on any previous date
      const prevAttempts = await sql`
        SELECT COUNT(*) as cnt, MIN(attempt_date) as first_date
        FROM task_attempt_history
        WHERE task_id = ${t.task_id}
        AND attempt_date < ${date}::date
      `
      const prevCount = parseInt(prevAttempts[0]?.cnt || 0)
      const firstDate = prevAttempts[0]?.first_date || date
      const attemptNum = prevCount + 1
      const daysSince = Math.floor((new Date(date) - new Date(firstDate)) / 86400000)

      await sql`
        INSERT INTO task_attempt_history
          (task_id, attempt_number, attempt_date, status, root_cause,
           rider_id, vehicle_id, planned_tour_name, is_final_success, days_since_first)
        VALUES
          (${t.task_id}, ${attemptNum}, ${date}::date, ${t.task_status},
           ${t.root_cause || ''}, ${t.rider_id || ''}, ${t.vehicle_id || ''},
           ${t.planned_tour_name || ''}, ${t.is_completed}, ${daysSince})
        ON CONFLICT (task_id, attempt_date) DO UPDATE SET
          status = EXCLUDED.status,
          attempt_number = EXCLUDED.attempt_number,
          is_final_success = EXCLUDED.is_final_success,
          days_since_first = EXCLUDED.days_since_first
      `
    }

    // ── Run KPI engine ────────────────────────────────────────────────────────
    const result = await recalculateDay(date)

    return Response.json({
      status: 'success',
      result,
      attempt_history_updated: tasksForDate.length
    })
  } catch (err) {
    console.error('[recalculate] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}
