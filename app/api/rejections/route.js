// app/api/rejections/route.js
// Returns top rejection reasons + top defaulting clients for a given date range
// Query params:
//   date_from  (required)  — YYYY-MM-DD
//   date_to    (optional)  — YYYY-MM-DD  (defaults to date_from → single day)
//   top_n      (optional)  — how many reasons to return (default 3)

import { neon } from '@neondatabase/serverless'

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const dateFrom = searchParams.get('date_from')
  const dateTo   = searchParams.get('date_to') || dateFrom
  const topNParam = searchParams.get('top_n') || '3'
  const topN     = topNParam === 'all' ? 999 : parseInt(topNParam, 10)

  if (!dateFrom) {
    return Response.json({ error: 'date_from is required' }, { status: 400 })
  }

  try {
    const sql = neon(process.env.DATABASE_URL)

    // ── 1. Top N rejection reasons in the date range ──────────────────────────
    const reasons = await sql`
      SELECT
        COALESCE(NULLIF(TRIM(root_cause), ''), 'Unknown') AS reason,
        COUNT(*)                                           AS total_failed,
        COUNT(DISTINCT customer_name)                      AS unique_clients,
        ROUND(
          COUNT(*)::NUMERIC /
          NULLIF((
            SELECT COUNT(*) FROM raw_tasks
            WHERE dispatch_date BETWEEN ${dateFrom}::date AND ${dateTo}::date
              AND is_failed = TRUE
          ), 0) * 100
        , 1)                                               AS pct_of_all_failures
      FROM raw_tasks
      WHERE dispatch_date BETWEEN ${dateFrom}::date AND ${dateTo}::date
        AND is_failed = TRUE
      GROUP BY reason
      ORDER BY total_failed DESC
      LIMIT ${topN}
    `

    if (reasons.length === 0) {
      return Response.json({
        date_from: dateFrom,
        date_to:   dateTo,
        total_failed: 0,
        reasons: [],
      })
    }

    // ── 2. Top clients per reason (parallel queries) ──────────────────────────
    const reasonNames = reasons.map(r => r.reason)

    const clientRows = await sql`
      SELECT
        COALESCE(NULLIF(TRIM(root_cause), ''), 'Unknown')   AS reason,
        COALESCE(NULLIF(TRIM(customer_name), ''), 'Unknown') AS client,
        COUNT(*)                                             AS failed_count,
        SUM(invoice_value)                                   AS invoice_value_lost,
        city,
        zone
      FROM raw_tasks
      WHERE dispatch_date BETWEEN ${dateFrom}::date AND ${dateTo}::date
        AND is_failed = TRUE
        AND COALESCE(NULLIF(TRIM(root_cause), ''), 'Unknown') = ANY(${reasonNames})
      GROUP BY reason, client, city, zone
      ORDER BY reason, failed_count DESC
    `

    // ── Root causes breakdown per reason ──────────────────────────────────────
    const rootCauseRows = await sql`
      SELECT
        COALESCE(NULLIF(TRIM(root_cause), ''), 'Unknown') AS reason,
        task_status                                        AS root_cause_detail,
        COUNT(*)                                           AS count
      FROM raw_tasks
      WHERE dispatch_date BETWEEN ${dateFrom}::date AND ${dateTo}::date
        AND is_failed = TRUE
        AND COALESCE(NULLIF(TRIM(root_cause), ''), 'Unknown') = ANY(${reasonNames})
      GROUP BY reason, task_status
      ORDER BY reason, count DESC
    `

    // ── 3. Aggregate: total failed count for the period ───────────────────────
    const [{ total_failed }] = await sql`
      SELECT COUNT(*) AS total_failed
      FROM raw_tasks
      WHERE dispatch_date BETWEEN ${dateFrom}::date AND ${dateTo}::date
        AND is_failed = TRUE
    `

    // ── 4. Daily trend per reason (for sparkline) ─────────────────────────────
    const trend = await sql`
      SELECT
        dispatch_date::text                                 AS date,
        COALESCE(NULLIF(TRIM(root_cause), ''), 'Unknown')  AS reason,
        COUNT(*)                                            AS count
      FROM raw_tasks
      WHERE dispatch_date BETWEEN ${dateFrom}::date AND ${dateTo}::date
        AND is_failed = TRUE
        AND COALESCE(NULLIF(TRIM(root_cause), ''), 'Unknown') = ANY(${reasonNames})
      GROUP BY dispatch_date, reason
      ORDER BY dispatch_date ASC
    `

    // ── 5. Shape response ─────────────────────────────────────────────────────
    const TOP_CLIENTS = 10

    const response = {
      date_from:    dateFrom,
      date_to:      dateTo,
      total_failed: Number(total_failed),
      reasons: reasons.map(r => {
        const clients = clientRows
          .filter(c => c.reason === r.reason)
          .slice(0, TOP_CLIENTS)
          .map(c => ({
            client:             c.client,
            failed_count:       Number(c.failed_count),
            invoice_value_lost: c.invoice_value_lost ? Number(c.invoice_value_lost) : null,
            city:               c.city || '',
            zone:               c.zone || '',
          }))

        const reasonTrend = trend
          .filter(t => t.reason === r.reason)
          .map(t => ({ date: t.date, count: Number(t.count) }))

        const rootCauses = rootCauseRows
          .filter(rc => rc.reason === r.reason)
          .map(rc => ({ detail: rc.root_cause_detail, count: Number(rc.count) }))

        return {
          reason:              r.reason,
          total_failed:        Number(r.total_failed),
          unique_clients:      Number(r.unique_clients),
          pct_of_all_failures: Number(r.pct_of_all_failures),
          top_clients:         clients,
          trend:               reasonTrend,
          root_causes:         rootCauses,
        }
      }),
    }

    return Response.json(response)
  } catch (err) {
    console.error('[/api/rejections] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}
