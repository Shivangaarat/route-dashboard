// app/api/backup/route.js
// GET  — download full database backup as Excel
// POST — trigger manual backup log entry

import { neon } from '@neondatabase/serverless'

export const runtime = 'nodejs'
export const maxDuration = 60

function db() { return neon(process.env.DATABASE_URL) }

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const format = searchParams.get('format') || 'xlsx'

  try {
    const sql = db()
    const backupId = await sql`
      INSERT INTO database_backups (backup_type, triggered_by, status)
      VALUES ('manual', 'dashboard', 'running')
      RETURNING id
    `
    const bid = backupId[0].id

    const [tasks, tourMetrics, summary, vehicles, exclusions, settingsData, attempts] = await Promise.all([
      sql`SELECT * FROM raw_tasks ORDER BY dispatch_date DESC, planned_tour_name`,
      sql`SELECT * FROM daily_tour_metrics ORDER BY dispatch_date DESC`,
      sql`SELECT * FROM daily_summary ORDER BY dispatch_date DESC, analysis_category`,
      sql`SELECT * FROM vehicle_master ORDER BY vehicle_id`,
      sql`SELECT * FROM route_exclusions ORDER BY pattern`,
      sql`SELECT * FROM settings ORDER BY key`,
      sql`SELECT * FROM task_attempt_history ORDER BY attempt_date DESC, task_id`,
    ])

    const ExcelJS = (await import('exceljs')).default
    const wb = new ExcelJS.Workbook()
    wb.creator = 'Route Dashboard Backup'
    wb.created = new Date()

    function addSheet(name, data) {
      if (!data.length) return
      const ws = wb.addWorksheet(name)
      const cols = Object.keys(data[0])
      ws.columns = cols.map(c => ({ header: c, key: c, width: Math.min(Math.max(c.length + 4, 12), 40) }))
      const hRow = ws.getRow(1)
      hRow.eachCell(cell => {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1F3864' } }
        cell.font = { color: { argb: 'FFFFFFFF' }, bold: true }
      })
      data.forEach(row => {
        const r = {}
        cols.forEach(c => { r[c] = row[c] instanceof Date ? row[c].toISOString() : row[c] })
        ws.addRow(r)
      })
    }

    addSheet('Raw Tasks',         tasks)
    addSheet('Tour Metrics',      tourMetrics)
    addSheet('Daily Summary',     summary)
    addSheet('Vehicle Master',    vehicles)
    addSheet('Route Exclusions',  exclusions)
    addSheet('Settings',          settingsData)
    addSheet('Attempt History',   attempts)

    // Summary sheet
    const sumWs = wb.addWorksheet('Backup Info')
    sumWs.addRow(['Route Pattern Intelligence Dashboard — Full Backup'])
    sumWs.addRow(['Generated at', new Date().toISOString()])
    sumWs.addRow(['Raw tasks', tasks.length])
    sumWs.addRow(['Tour metrics', tourMetrics.length])
    sumWs.addRow(['Daily summaries', summary.length])
    sumWs.addRow(['Vehicles', vehicles.length])
    sumWs.addRow(['Attempt records', attempts.length])

    const buffer = await wb.xlsx.writeBuffer()
    const sizeKB = Math.round(buffer.byteLength / 1024)

    await sql`
      UPDATE database_backups SET
        status = 'success', completed_at = NOW(),
        raw_task_count = ${tasks.length},
        tour_metric_count = ${tourMetrics.length},
        summary_count = ${summary.length},
        file_size_kb = ${sizeKB}
      WHERE id = ${bid}
    `

    const today = new Date().toISOString().split('T')[0]
    return new Response(buffer, {
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="route_dashboard_backup_${today}.xlsx"`,
      }
    })

  } catch (err) {
    console.error('[backup] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}

export async function POST() {
  try {
    const sql = db()
    const history = await sql`
      SELECT id, backup_type, triggered_at::text, completed_at::text,
             status, raw_task_count, file_size_kb, triggered_by
      FROM database_backups
      ORDER BY triggered_at DESC LIMIT 20
    `
    return Response.json({ history })
  } catch (err) {
    return Response.json({ error: err.message }, { status: 500 })
  }
}
