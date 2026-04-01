// app/api/analytics/route.js
// GET /api/analytics?view=daily&date=2026-03-28&category=Overall&ou=All
// GET /api/analytics?view=mtd&month=2026-03&ou=All
// GET /api/analytics?view=ytd&year=2026&ou=All
// GET /api/analytics?view=emirates&date=2026-03-28&ou=All
// GET /api/analytics?view=redelivery
// GET /api/analytics?view=dates

import { neon } from '@neondatabase/serverless'

export const runtime = 'nodejs'
function db() { return neon(process.env.DATABASE_URL) }

// ── Aggregate tour metrics into a summary row (mirrors kpi-engine logic) ──────
function aggregateTours(tourRows, cat) {
  const cm = cat === 'Overall' ? tourRows : tourRows.filter(m => m.analysis_category === cat)
  if (!cm.length) return null
  const included = cm.filter(m => !m.is_excluded && !m.is_virtual_vehicle)
  const own      = included.filter(m => m.ownership === 'OWN')
  const dleased  = included.filter(m => m.ownership === 'D-LEASED')
  const single   = included.filter(m => m.route_type === 'Single')
  const multi    = included.filter(m => m.route_type === 'Multi')
  const bulk     = included.filter(m => m.is_bulk)

  const totalDrops  = included.reduce((s,m)=>s+Number(m.unique_drops||0),0)
  const ownDrops    = own.reduce((s,m)=>s+Number(m.unique_drops||0),0)
  const dlDrops     = dleased.reduce((s,m)=>s+Number(m.unique_drops||0),0)
  const totalOrders = cm.reduce((s,m)=>s+Number(m.total_orders||0),0)
  const completed   = cm.reduce((s,m)=>s+Number(m.completed_orders||0),0)
  const failed      = cm.reduce((s,m)=>s+Number(m.failed_orders||0),0)

  const overallAvg = included.length ? parseFloat((totalDrops/included.length).toFixed(2)) : 0
  const multiAvg   = multi.length ? parseFloat((multi.reduce((s,m)=>s+Number(m.unique_drops||0),0)/multi.length).toFixed(2)) : 0
  const ownAvg     = own.length ? parseFloat((ownDrops/own.length).toFixed(2)) : 0
  const dlAvg      = dleased.length ? parseFloat((dlDrops/dleased.length).toFixed(2)) : 0
  const utilTours  = included.filter(m => m.volume_util_pct != null && !isNaN(Number(m.volume_util_pct)))
  const avgUtil    = utilTours.length ? parseFloat((utilTours.reduce((s,m)=>s+Number(m.volume_util_pct),0)/utilTours.length).toFixed(2)) : null
  const rejPct     = totalOrders > 0 ? parseFloat((failed/totalOrders*100).toFixed(2)) : 0
  const firstAtt   = totalOrders > 0 ? parseFloat((completed/totalOrders*100).toFixed(2)) : 0

  return {
    analysis_category:          cat,
    total_tours:                cm.length,
    included_tours:             included.length,
    excluded_tours:             cm.filter(m=>m.is_excluded||m.is_virtual_vehicle).length,
    own_vehicles:               own.length,
    dleased_vehicles:           dleased.length,
    total_drops:                totalDrops,
    own_drops:                  ownDrops,
    dleased_drops:              dlDrops,
    own_avg_drops:              ownAvg,
    dleased_avg_drops:          dlAvg,
    overall_avg_drops:          overallAvg,
    avg_drops_excl_single:      multiAvg,
    single_drop_count:          single.length,
    multi_drop_vehicle_count:   multi.length,
    multi_drop_total:           multi.reduce((s,m)=>s+Number(m.unique_drops||0),0),
    single_drop_vehicle_count:  single.length,
    single_drop_total:          single.reduce((s,m)=>s+Number(m.unique_drops||0),0),
    total_orders:               totalOrders,
    completed_orders:           completed,
    failed_orders:              failed,
    daily_rejection_pct:        rejPct,
    avg_volume_util_pct:        avgUtil,
    avg_pallet_util_pct:        null,
    bulk_route_count:           bulk.length,
    first_attempt_success_pct:  firstAtt,
    rd_pct:                     null,
    rd_pharma_pct:              null,
    rd_medlab_pct:              null,
  }
}

// Build summary for all 4 categories from tour rows
function buildSummaryFromTours(tourRows) {
  return ['Overall','NHC Ambient','NHC Frozen','HC']
    .map(cat => aggregateTours(tourRows, cat))
    .filter(Boolean)
}

// Build emirates breakdown from tour rows
function buildEmiratesFromTours(tourRows) {
  const rows = []
  const cities = [...new Set(tourRows.map(m => m.city).filter(Boolean))]
  for (const city of cities) {
    for (const cat of ['Overall','NHC Ambient','NHC Frozen','HC']) {
      const cm = (cat === 'Overall' ? tourRows : tourRows.filter(m=>m.analysis_category===cat))
        .filter(m => m.city === city)
      if (!cm.length) continue
      const orders = cm.reduce((s,m)=>s+Number(m.total_orders||0),0)
      const drops  = cm.reduce((s,m)=>s+Number(m.unique_drops||0),0)
      const comp   = cm.reduce((s,m)=>s+Number(m.completed_orders||0),0)
      const fail   = cm.reduce((s,m)=>s+Number(m.failed_orders||0),0)
      const vol    = cm.reduce((s,m)=>s+Number(m.total_volume_cbm||0),0)
      rows.push({
        analysis_category: cat, city,
        total_orders: orders, total_drops: drops,
        completed_orders: comp, failed_orders: fail,
        total_volume_cbm: parseFloat(vol.toFixed(3)),
        rejection_pct: orders > 0 ? parseFloat((fail/orders*100).toFixed(2)) : 0
      })
    }
  }
  return rows
}

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const view     = searchParams.get('view') || 'dates'
  const date     = searchParams.get('date')
  const month    = searchParams.get('month')
  const year     = searchParams.get('year') || new Date().getFullYear()
  const category = searchParams.get('category') || 'Overall'
  const ou       = searchParams.get('ou') || 'All'
  const ouFilter = ou !== 'All'

  try {
    const sql = db()

    // ── Available dates ─────────────────────────────────────────────────────
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
      const ouRows = await sql`
        SELECT DISTINCT operating_unit AS ou
        FROM daily_tour_metrics
        WHERE operating_unit IS NOT NULL AND operating_unit != ''
        ORDER BY operating_unit
      `
      const allOUs = [...new Set(
        ouRows.flatMap(r => r.ou.split(',').map(s => s.trim()).filter(Boolean))
      )].sort()
      return Response.json({ dates: rows, operating_units: allOUs })
    }

    // ── Daily view ──────────────────────────────────────────────────────────
    if (view === 'daily') {
      if (!date) return Response.json({ error: 'date required' }, { status: 400 })
      const dateTo  = searchParams.get('dateTo') || date
      const isRange = dateTo !== date

      if (!isRange) {
        if (!ouFilter) {
          // No OU filter — use pre-calculated summary tables
          const [summary, tours, emiratesData] = await Promise.all([
            sql`SELECT * FROM daily_summary WHERE dispatch_date = ${date} ORDER BY analysis_category`,
            sql`SELECT * FROM daily_tour_metrics WHERE dispatch_date = ${date}
                AND (analysis_category = ${category} OR ${category} = 'Overall')
                ORDER BY is_bulk DESC, unique_drops DESC`,
            sql`SELECT * FROM emirates_daily WHERE dispatch_date = ${date}
                AND analysis_category = ${category} ORDER BY total_drops DESC`
          ])
          return Response.json({ date, dateTo, summary, tours, emirates: emiratesData })
        }

        // OU filter — recalculate summary from filtered tours
        const tours = await sql`
          SELECT * FROM daily_tour_metrics
          WHERE dispatch_date = ${date}
          AND operating_unit ILIKE '%' || ${ou} || '%'
          ORDER BY is_bulk DESC, unique_drops DESC
        `
        const summary    = buildSummaryFromTours(tours)
        const emiratesData = buildEmiratesFromTours(tours)
          .filter(r => category === 'Overall' || r.analysis_category === category)
        return Response.json({ date, dateTo, summary, tours, emirates: emiratesData, ou })
      }

      // Date range
      if (!ouFilter) {
        const [summary, tours, emiratesData] = await Promise.all([
          sql`
            SELECT analysis_category,
              COUNT(DISTINCT dispatch_date)                                        AS days_active,
              SUM(included_tours)                                                  AS included_tours,
              SUM(excluded_tours)                                                  AS excluded_tours,
              SUM(own_vehicles)                                                    AS own_vehicles,
              SUM(dleased_vehicles)                                                AS dleased_vehicles,
              SUM(total_drops)                                                     AS total_drops,
              SUM(own_drops)                                                       AS own_drops,
              SUM(dleased_drops)                                                   AS dleased_drops,
              ROUND(AVG(own_avg_drops),2)                                          AS own_avg_drops,
              ROUND(AVG(dleased_avg_drops),2)                                      AS dleased_avg_drops,
              ROUND(AVG(overall_avg_drops),2)                                      AS overall_avg_drops,
              ROUND(AVG(avg_drops_excl_single),2)                                  AS avg_drops_excl_single,
              SUM(single_drop_count)                                               AS single_drop_count,
              SUM(multi_drop_vehicle_count)                                        AS multi_drop_vehicle_count,
              SUM(multi_drop_total)                                                AS multi_drop_total,
              SUM(single_drop_vehicle_count)                                       AS single_drop_vehicle_count,
              SUM(single_drop_total)                                               AS single_drop_total,
              SUM(total_orders)                                                    AS total_orders,
              SUM(completed_orders)                                                AS completed_orders,
              SUM(failed_orders)                                                   AS failed_orders,
              ROUND(SUM(failed_orders)::numeric/NULLIF(SUM(total_orders),0)*100,2) AS daily_rejection_pct,
              ROUND(AVG(avg_volume_util_pct),2)                                    AS avg_volume_util_pct,
              ROUND(AVG(avg_pallet_util_pct),2)                                    AS avg_pallet_util_pct,
              SUM(bulk_route_count)                                                AS bulk_route_count,
              ROUND(AVG(rd_pct),2)                                                 AS rd_pct,
              ROUND(AVG(rd_pharma_pct),2)                                          AS rd_pharma_pct,
              ROUND(AVG(rd_medlab_pct),2)                                          AS rd_medlab_pct,
              ROUND(AVG(first_attempt_success_pct),2)                              AS first_attempt_success_pct
            FROM daily_summary
            WHERE dispatch_date >= ${date}::date AND dispatch_date <= ${dateTo}::date
            GROUP BY analysis_category ORDER BY analysis_category
          `,
          sql`
            SELECT * FROM daily_tour_metrics
            WHERE dispatch_date >= ${date}::date AND dispatch_date <= ${dateTo}::date
            AND (analysis_category = ${category} OR ${category} = 'Overall')
            ORDER BY dispatch_date DESC, is_bulk DESC, unique_drops DESC LIMIT 500
          `,
          sql`
            SELECT analysis_category, city,
              SUM(total_orders) AS total_orders, SUM(total_drops) AS total_drops,
              SUM(completed_orders) AS completed_orders, SUM(failed_orders) AS failed_orders,
              SUM(total_volume_cbm) AS total_volume_cbm,
              ROUND(SUM(failed_orders)::numeric/NULLIF(SUM(total_orders),0)*100,2) AS rejection_pct
            FROM emirates_daily
            WHERE dispatch_date >= ${date}::date AND dispatch_date <= ${dateTo}::date
            AND analysis_category = ${category}
            GROUP BY analysis_category, city ORDER BY total_drops DESC
          `
        ])
        return Response.json({ date, dateTo, isRange: true, summary, tours, emirates: emiratesData })
      }

      // Date range + OU filter
      const tours = await sql`
        SELECT * FROM daily_tour_metrics
        WHERE dispatch_date >= ${date}::date AND dispatch_date <= ${dateTo}::date
        AND operating_unit ILIKE '%' || ${ou} || '%'
        ORDER BY dispatch_date DESC, is_bulk DESC, unique_drops DESC LIMIT 500
      `
      const summary    = buildSummaryFromTours(tours)
      const emiratesData = buildEmiratesFromTours(tours)
        .filter(r => category === 'Overall' || r.analysis_category === category)
      return Response.json({ date, dateTo, isRange: true, summary, tours, emirates: emiratesData, ou })
    }

    // ── MTD view ────────────────────────────────────────────────────────────
    if (view === 'mtd') {
      const monthFilter = month || new Date().toISOString().slice(0, 7)

      if (!ouFilter) {
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

      // MTD + OU filter — build from tour metrics
      const tours = await sql`
        SELECT * FROM daily_tour_metrics
        WHERE dispatch_date >= ${monthFilter + '-01'}::date
        AND dispatch_date < (${monthFilter + '-01'}::date + interval '1 month')
        AND operating_unit ILIKE '%' || ${ou} || '%'
      `
      const summary = buildSummaryFromTours(tours)

      // Daily breakdown grouped by date
      const dateMap = {}
      tours.forEach(t => {
        const d = t.dispatch_date instanceof Date ? t.dispatch_date.toISOString().split('T')[0] : String(t.dispatch_date).split('T')[0]
        if (!dateMap[d]) dateMap[d] = []
        dateMap[d].push(t)
      })
      const daily = Object.entries(dateMap).sort(([a],[b])=>a.localeCompare(b)).map(([d, rows]) => {
        const agg = aggregateTours(rows, category === 'Overall' ? 'Overall' : category)
        return agg ? { ...agg, dispatch_date: d } : null
      }).filter(Boolean)

      return Response.json({ month: monthFilter, summary, daily, ou })
    }

    // ── YTD view ────────────────────────────────────────────────────────────
    if (view === 'ytd') {
      if (!ouFilter) {
        const rows = await sql`
          SELECT * FROM ytd_summary WHERE year = ${parseInt(year)}
          ORDER BY month_start, analysis_category
        `
        return Response.json({ year, summary: rows })
      }

      // YTD + OU filter
      const tours = await sql`
        SELECT *, EXTRACT(YEAR FROM dispatch_date) as yr,
               TO_CHAR(dispatch_date, 'Mon-YY') as month_label,
               DATE_TRUNC('month', dispatch_date)::date as month_start
        FROM daily_tour_metrics
        WHERE EXTRACT(YEAR FROM dispatch_date) = ${parseInt(year)}
        AND operating_unit ILIKE '%' || ${ou} || '%'
      `
      // Group by month
      const monthMap = {}
      tours.forEach(t => {
        const key = t.month_label
        if (!monthMap[key]) monthMap[key] = { month_label: key, month_start: t.month_start, rows: [] }
        monthMap[key].rows.push(t)
      })
      const summary = Object.values(monthMap).sort((a,b)=>a.month_start>b.month_start?1:-1).flatMap(({ month_label, month_start, rows }) =>
        ['Overall','NHC Ambient','NHC Frozen','HC'].map(cat => {
          const agg = aggregateTours(rows, cat)
          if (!agg) return null
          return { ...agg, month_label, month_start, year: parseInt(year),
            ytd_rejection_pct: agg.daily_rejection_pct,
            ytd_rd_pct: agg.rd_pct,
            avg_cbm_util_pct: agg.avg_volume_util_pct,
          }
        }).filter(Boolean)
      )
      return Response.json({ year, summary, ou })
    }

    // ── Emirates view ────────────────────────────────────────────────────────
    if (view === 'emirates') {
      if (!date && !month) return Response.json({ error: 'date or month required' }, { status: 400 })
      if (date) {
        if (!ouFilter) {
          const rows = await sql`SELECT * FROM emirates_daily WHERE dispatch_date = ${date} ORDER BY analysis_category, total_drops DESC`
          return Response.json({ date, emirates: rows })
        }
        const tours = await sql`
          SELECT * FROM daily_tour_metrics
          WHERE dispatch_date = ${date}
          AND operating_unit ILIKE '%' || ${ou} || '%'
        `
        return Response.json({ date, emirates: buildEmiratesFromTours(tours), ou })
      }
      if (!ouFilter) {
        const rows = await sql`SELECT * FROM emirates_mtd WHERE month_label = ${month} ORDER BY analysis_category, total_drops DESC`
        return Response.json({ month, emirates: rows })
      }
      const tours = await sql`
        SELECT * FROM daily_tour_metrics
        WHERE TO_CHAR(dispatch_date, 'Mon-YY') = ${month}
        AND operating_unit ILIKE '%' || ${ou} || '%'
      `
      return Response.json({ month, emirates: buildEmiratesFromTours(tours), ou })
    }

    // ── Redelivery view ──────────────────────────────────────────────────────
    if (view === 'redelivery') {
      const rows = await sql`
        SELECT task_id, total_attempts, first_attempt_date::text,
               last_attempt_date::text, days_to_deliver, final_status, was_delivered
        FROM redelivery_summary
        ORDER BY total_attempts DESC, days_to_deliver DESC LIMIT 200
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
