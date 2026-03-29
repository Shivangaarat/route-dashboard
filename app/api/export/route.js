// app/api/export/route.js
// GET /api/export?date=X&format=xlsx|csv
// Generates a downloadable report from any stored snapshot

import { getSnapshot } from '../../../lib/db.js'
import { detectPatterns, computeDriverScores } from '../../../lib/engine.js'

export const runtime = 'nodejs'
export const maxDuration = 30

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const date   = searchParams.get('date') || new Date().toISOString().split('T')[0]
  const format = searchParams.get('format') || 'xlsx'

  const snapshot = await getSnapshot(date)
  if (!snapshot) {
    return Response.json({ error: `No data for ${date}` }, { status: 404 })
  }

  const data = snapshot.data
  const patterns = detectPatterns(data)
  const driverScores = computeDriverScores(data)

  if (format === 'csv') {
    const csv = buildCSV(data)
    return new Response(csv, {
      headers: {
        'Content-Type': 'text/csv',
        'Content-Disposition': `attachment; filename="dispatch_${date}.csv"`,
      },
    })
  }

  // Build Excel with multiple sheets
  const ExcelJS = (await import('exceljs')).default
  const wb = new ExcelJS.Workbook()
  wb.creator = 'Route Dashboard'
  wb.created = new Date()

  // ── Sheet 1: Raw dispatch data ──────────────────────────
  const wsRaw = wb.addWorksheet('Dispatch Data')
  wsRaw.columns = [
    { header: 'Route ID',    key: 'route_id',    width: 14 },
    { header: 'Route Name',  key: 'route_name',  width: 18 },
    { header: 'Date',        key: 'date',         width: 12 },
    { header: 'Day',         key: 'day',          width: 8  },
    { header: 'Zone',        key: 'zone',         width: 10 },
    { header: 'Team',        key: 'team',         width: 16 },
    { header: 'Driver ID',   key: 'driver_id',    width: 12 },
    { header: 'Sites',       key: 'sites',        width: 24 },
    { header: 'Volume',      key: 'volume',       width: 10 },
    { header: 'Drops',       key: 'drops',        width: 8  },
    { header: 'Successful',  key: 'successful',   width: 12 },
    { header: 'Rejections',  key: 'rejections',   width: 12 },
    { header: 'Helpers',     key: 'helpers',      width: 10 },
    { header: 'Rej Reason',  key: 'rej_reason',   width: 22 },
  ]
  styleHeader(wsRaw)
  data.forEach(r => wsRaw.addRow(r))

  // ── Sheet 2: Route patterns ─────────────────────────────
  const wsPat = wb.addWorksheet('Route Patterns')
  wsPat.columns = [
    { header: 'Route Name',    key: 'name',         width: 18 },
    { header: 'Zone',          key: 'zone',         width: 10 },
    { header: 'Team',          key: 'team',         width: 16 },
    { header: 'Sites',         key: 'sites',        width: 24 },
    { header: 'Occurrences',   key: 'occurrences',  width: 14 },
    { header: 'Avg Volume',    key: 'avgVol',       width: 12 },
    { header: 'Std Dev Vol',   key: 'stdVol',       width: 12 },
    { header: 'Avg Drops',     key: 'avgDrops',     width: 11 },
    { header: 'Avg Helpers',   key: 'avgHelpers',   width: 12 },
    { header: 'Rejection %',   key: 'rejPct',       width: 12 },
    { header: 'Confidence %',  key: 'confidence',   width: 14 },
    { header: 'Top Driver',    key: 'topDriver',    width: 12 },
    { header: 'Action',        key: 'action',       width: 14 },
  ]
  styleHeader(wsPat)
  patterns.forEach(p => {
    const row = wsPat.addRow({ ...p, action: p.confidence >= 80 ? 'Auto-plan' : p.confidence >= 70 ? 'Review' : 'Not ready' })
    const confCell = row.getCell('confidence')
    if (p.confidence >= 70) confCell.font = { color: { argb: 'FF1D9E75' }, bold: true }
    else if (p.confidence >= 40) confCell.font = { color: { argb: 'FFBA7517' }, bold: true }
    else confCell.font = { color: { argb: 'FFE24B4A' }, bold: true }
  })

  // ── Sheet 3: Driver scores ──────────────────────────────
  const wsDrv = wb.addWorksheet('Driver Scores')
  wsDrv.columns = [
    { header: 'Rank',         key: 'rank',         width: 8  },
    { header: 'Driver ID',    key: 'id',           width: 12 },
    { header: 'Driver Name',  key: 'name',         width: 20 },
    { header: 'Final Score',  key: 'finalScore',   width: 13 },
    { header: 'Tier',         key: 'tier',         width: 12 },
    { header: 'Completion',   key: 'avgCompRate',  width: 13 },
    { header: 'Rejection %',  key: 'avgRejRate',   width: 13 },
    { header: 'Days Active',  key: 'daysActive',   width: 12 },
    { header: 'Total Drops',  key: 'totalDrops',   width: 12 },
    { header: 'Successful',   key: 'totalSuccessful', width: 12 },
    { header: 'Total Volume', key: 'totalVol',     width: 14 },
    { header: 'Trend',        key: 'trend',        width: 10 },
    { header: 'Comp Score',   key: 'completion',   width: 12 },
    { header: 'Vol Score',    key: 'volume',       width: 12 },
    { header: 'Rej Score',    key: 'rejection',    width: 12 },
    { header: 'Consistency',  key: 'consistency',  width: 14 },
  ]
  styleHeader(wsDrv)
  driverScores.forEach((d, i) => {
    const row = wsDrv.addRow({
      rank: i + 1, ...d,
      completion:  d.scores.completion,
      volume:      d.scores.volume,
      rejection:   d.scores.rejection,
      consistency: d.scores.consistency,
    })
    const tierColors = { Elite: 'FF1D9E75', Strong: 'FF378ADD', Developing: 'FFBA7517', 'At Risk': 'FFE24B4A' }
    const c = tierColors[d.tier]
    if (c) row.getCell('finalScore').font = { color: { argb: c }, bold: true }
  })

  const buffer = await wb.xlsx.writeBuffer()
  return new Response(buffer, {
    headers: {
      'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'Content-Disposition': `attachment; filename="route_report_${date}.xlsx"`,
    },
  })
}

function styleHeader(ws) {
  const row = ws.getRow(1)
  row.eachCell(cell => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1F3864' } }
    cell.font = { color: { argb: 'FFFFFFFF' }, bold: true }
    cell.alignment = { vertical: 'middle' }
  })
  row.height = 18
}

function buildCSV(rows) {
  if (!rows.length) return ''
  const headers = Object.keys(rows[0])
  const lines = [headers.join(',')]
  rows.forEach(r => {
    lines.push(headers.map(h => {
      const v = String(r[h] ?? '').replace(/"/g, '""')
      return v.includes(',') || v.includes('"') || v.includes('\n') ? `"${v}"` : v
    }).join(','))
  })
  return lines.join('\n')
}
