// app/api/history/route.js
// GET /api/history          → list of available import dates
// GET /api/history?date=X   → full snapshot for that date

import { getAvailableDates, getSnapshot } from '../../../lib/db.js'
import { detectPatterns, computeDriverScores } from '../../../lib/engine.js'

export const runtime = 'nodejs'

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const date = searchParams.get('date')

  try {
    if (!date) {
      // Return list of all available dates
      const dates = await getAvailableDates()
      return Response.json({ dates })
    }

    // Return full analytics for a specific date
    const snapshot = await getSnapshot(date)
    if (!snapshot) {
      return Response.json({ error: `No data found for ${date}` }, { status: 404 })
    }

    const data = snapshot.data
    const patterns = detectPatterns(data)
    const driverScores = computeDriverScores(data)

    // Summary KPIs
    const totalVol = data.reduce((s, r) => s + (Number(r.volume) || 0), 0)
    const totalDrops = data.reduce((s, r) => s + (Number(r.drops) || 0), 0)
    const totalRej = data.reduce((s, r) => s + (Number(r.rejections) || 0), 0)

    return Response.json({
      date,
      importedAt:  snapshot.importedAt,
      source:      snapshot.source,
      rowCount:    data.length,
      kpis: {
        totalDispatches: data.length,
        avgVolume:       data.length ? Math.round(totalVol / data.length) : 0,
        rejectionRate:   totalDrops ? +(totalRej / totalDrops * 100).toFixed(1) : 0,
        templateCount:   patterns.length,
        automatable:     patterns.filter(p => p.confidence >= 70).length,
        avgDriverScore:  driverScores.length
          ? +(driverScores.reduce((s, d) => s + d.finalScore, 0) / driverScores.length).toFixed(1)
          : 0,
      },
      patterns,
      driverScores,
      rawRows: data,
    })
  } catch (err) {
    console.error('[history] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}
