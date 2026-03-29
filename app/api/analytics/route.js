// app/api/analytics/route.js
// GET /api/analytics?view=daily&date=2026-03-28&category=Overall
// GET /api/analytics?view=mtd&month=2026-03
// GET /api/analytics?view=ytd&year=2026
// GET /api/analytics?view=emirates&date=2026-03-28
// GET /api/analytics?view=redelivery&date=2026-03-28
// GET /api/analytics?view=dates

import { neon } from '@neondatabase/serverless'

export const runtime = 'nodejs'
function db() { return neon(process.env.DATABASE_URL) }

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const view     = searchParams.get('view') || 'dates'
  const date     = searchParams.get('date')
  const month    = searchParams.get('month')
  const year     = searchParams.get('year') || new Date().getFullYear()
  const category = searchParams.get('category') || 'Overall'

  try {
    const sql = db()

    // ── Available dates ───────────────────────────────────────────────────────
    if (view === 'dates') {
      const rows = await sql`
        SELECT DISTINCT dispatch_date::text, COUNT(*) as tours,
               SUM(total_orders) as total_orders
        FROM daily_summary
        WHERE analysis_category = 'Overall'
        GROUP BY dispatch_date
        ORDER BY dispatch_date DESC
        LIMIT 90
      `
      return Response.json({ dates: rows })
    }

    // ── Daily view ────────────────────────────────────────────────────────────
    if (view === 'daily') {
      if (!date) return Response.json({ error: 'date required' }, { status: 400 })

      const [summary, tours, emiratesData] = await Promise.all([
        sql`
          SELECT * FROM daily_summary
          WHERE dispatch_date = ${date}
          ORDER BY analysis_category
        `,
        sql`
          SELECT * FROM daily_tour_metrics
          WHERE dispatch_date = ${date}
          AND (analysis_category = ${category} OR ${category} = 'Overall')
          ORDER BY is_bulk DESC, unique_drops DESC
        `,
        sql`
          SELECT * FROM emirates_daily
          WHERE dispatch_date = ${date}
          AND analysis_category = ${category}
          ORDER BY total_drops DESC
        `
      ])

      return Response.json({ date, summary, tours, emirates: emiratesData })
    }

    // ── MTD view ──────────────────────────────────────────────────────────────
    if (view === 'mtd') {
      const monthFilter = month || new Date().toISOString().slice(0, 7)
      const rows = await sql`
        SELECT * FROM mtd_summary
        WHERE month_start >= ${monthFilter + '-01'}::date
        AND month_start < (${monthFilter + '-01'}::date + interval '1 month')
        ORDER BY analysis_category
      `
      const daily = await sql`
        SELECT ds.dispatch_date::text, ds.analysis_category,
               ds.overall_avg_drops, ds.avg_drops_excl_single, ds.single_drop_count,
               ds.own_vehicles, ds.own_drops, ds.dleased_vehicles, ds.dleased_drops,
               ds.own_avg_drops, ds.dleased_avg_drops,
               ds.multi_drop_vehicle_count, ds.multi_drop_total,
               ds.single_drop_vehicle_count, ds.single_drop_total,
               ds.avg_volume_util_pct, ds.avg_pallet_util_pct,
               ds.daily_rejection_pct, ds.rd_pct, ds.rd_pharma_pct, ds.rd_medlab_pct,
               ds.first_attempt_success_pct, ds.bulk_route_count, ds.included_tours
        FROM daily_summary ds
        WHERE dispatch_date >= ${monthFilter + '-01'}::date
        AND dispatch_date < (${monthFilter + '-01'}::date + interval '1 month')
        AND analysis_category = ${category}
        ORDER BY dispatch_date
      `
      return Response.json({ month: monthFilter, summary: rows, daily })
    }

    // ── YTD view ──────────────────────────────────────────────────────────────
    if (view === 'ytd') {
      const rows = await sql`
        SELECT * FROM ytd_summary
        WHERE year = ${parseInt(year)}
        ORDER BY month_start, analysis_category
      `
      return Response.json({ year, summary: rows })
    }

    // ── Emirates view ─────────────────────────────────────────────────────────
    if (view === 'emirates') {
      if (!date && !month) return Response.json({ error: 'date or month required' }, { status: 400 })
      if (date) {
        const rows = await sql`
          SELECT * FROM emirates_daily
          WHERE dispatch_date = ${date}
          ORDER BY analysis_category, total_drops DESC
        `
        return Response.json({ date, emirates: rows })
      }
      const rows = await sql`SELECT * FROM emirates_mtd WHERE month_label = ${month} ORDER BY analysis_category, total_drops DESC`
      return Response.json({ month, emirates: rows })
    }

    // ── Redelivery view ───────────────────────────────────────────────────────
    if (view === 'redelivery') {
      const rows = await sql`
        SELECT task_id, total_attempts, first_attempt_date::text,
               last_attempt_date::text, days_to_deliver, final_status, was_delivered
        FROM redelivery_summary
        ORDER BY total_attempts DESC, days_to_deliver DESC
        LIMIT 200
      `
      const stats = await sql`
        SELECT
          COUNT(DISTINCT task_id) FILTER (WHERE total_attempts > 1) as redelivery_count,
          ROUND(AVG(total_attempts) FILTER (WHERE total_attempts > 1), 2) as avg_attempts,
          COUNT(DISTINCT task_id) FILTER (WHERE total_attempts = 1) as first_attempt_success,
          COUNT(DISTINCT task_id) as total_tracked
        FROM redelivery_summary
      `
      return Response.json({ redeliveries: rows, stats: stats[0] })
    }

    return Response.json({ error: 'Invalid view parameter' }, { status: 400 })

  } catch (err) {
    console.error('[analytics] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}
