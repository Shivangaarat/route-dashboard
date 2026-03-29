// lib/engine.js
// Pattern detection + driver scoring + Excel parser
// Column mapping updated for Locus dispatch export format

// ── Excel Parser ─────────────────────────────────────────────────────────────

export function parseExcelBuffer(buffer) {
  const XLSX = require('xlsx')
  const wb = XLSX.read(buffer, { type: 'buffer', cellDates: true })
  const ws = wb.Sheets[wb.SheetNames[0]]
  const raw = XLSX.utils.sheet_to_json(ws, { defval: '' })

  return raw.map(row => {
    // Strip BOM character from keys (Excel adds \uFEFF to first column)
    const cleaned = {}
    Object.keys(row).forEach(k => {
      cleaned[k.replace(/^\uFEFF/, '').trim()] = row[k]
    })

    const get = (...keys) => {
      for (const k of keys) {
        const val = cleaned[k]
        if (val !== undefined && val !== null && val !== '') return val
      }
      return ''
    }

    // Date: handles DD/MM/YYYY and YYYY-MM-DD
    const dateVal = get('DISPATCH DATE', 'DELIVERY DATE', 'date', 'Date', 'DATE')
    let dateStr = ''
    if (dateVal instanceof Date) {
      dateStr = dateVal.toISOString().split('T')[0]
    } else if (typeof dateVal === 'string' && dateVal.trim()) {
      const ddmm = dateVal.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/)
      if (ddmm) {
        dateStr = `${ddmm[3]}-${ddmm[2].padStart(2, '0')}-${ddmm[1].padStart(2, '0')}`
      } else {
        dateStr = dateVal.trim().split(' ')[0]
      }
    }

    // Route name — prefer PLANNED TOUR NAME, fall back to TOUR ID
    const routeName = String(
      get('PLANNED TOUR NAME', 'route_name', 'Route Name') ||
      get('TOUR ID', 'route_id', 'Route ID') || ''
    ).trim()

    // Task status → successful / rejected
    const taskStatus = String(
      get('TASK STATUS', 'TRANSACTION STATUS', 'VISIT STATUS') || ''
    ).toUpperCase().trim()
    const isCompleted = taskStatus === 'COMPLETED'
    const isFailed = ['FAILED', 'CANCELLED', 'REJECTED', 'NOT DELIVERED', 'UNDELIVERED']
      .some(s => taskStatus.includes(s))

    // Volume — Locus stores as CBM decimal, scale up for readability
    const rawVol = parseFloat(get('VOLUME', 'volume', 'Volume') || 0)
    const volume = rawVol > 0 && rawVol < 1 ? +(rawVol * 1000).toFixed(3) : rawVol

    return {
      route_id:    String(get('TOUR ID', 'route_id', 'Route ID') || '').trim(),
      route_name:  routeName,
      date:        dateStr,
      day:         getDow(dateStr),
      zone:        String(get('ZONE', 'zone', 'Zone', 'CITY AREA') || 'Unknown').trim(),
      team:        String(get('TEAM NAME', 'team', 'Team') || 'Unknown').trim(),
      driver_id:   String(get('RIDER ID', 'driver_id', 'Driver ID') || '').trim(),
      driver_name: String(get('RIDER NAME', 'driver_name', 'Driver Name') || '').trim(),
      sites:       String(get('LOCATION ID', 'sites', 'Sites') || '').trim(),
      city:        String(get('CITY', 'city') || '').trim(),
      location:    String(get('LOCATION NAME', 'PLACE NAME', 'CUSTOMER NAME') || '').trim(),
      volume,
      drops:       1,
      successful:  isCompleted ? 1 : 0,
      rejections:  isFailed ? 1 : 0,
      helpers:     parseInt(get('helpers', 'Helpers') || 1) || 1,
      rej_reason:  String(get('ROOT CAUSE', 'Cancel reason', 'rej_reason') || '').trim(),
    }
  }).filter(r => r.date && r.date.length === 10 && (r.route_name || r.driver_id))
}

function getDow(ds) {
  if (!ds) return ''
  try {
    const d = new Date(ds)
    return ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][d.getDay()] || ''
  } catch { return '' }
}

// ── Pattern Detection ─────────────────────────────────────────────────────────

export function detectPatterns(data) {
  const map = {}
  data.forEach(r => {
    const key = r.route_name || r.route_id || 'unknown'
    if (!map[key]) map[key] = { name: r.route_name, sites: '', zone: r.zone, team: r.team, rows: [] }
    map[key].rows.push(r)
    if (r.sites && !map[key].sites.split('|').includes(r.sites)) {
      map[key].sites = map[key].sites ? map[key].sites + '|' + r.sites : r.sites
    }
  })

  return Object.values(map).map(p => {
    const rows = p.rows
    const vols = rows.map(r => Number(r.volume) || 0)
    const avgVol = Math.round(vols.reduce((a, b) => a + b, 0) / vols.length)
    const stdVol = Math.round(Math.sqrt(vols.map(v => (v - avgVol) ** 2).reduce((a, b) => a + b, 0) / vols.length))
    const avgDrops = +(rows.map(r => Number(r.drops) || 1).reduce((a, b) => a + b, 0) / rows.length).toFixed(1)
    const avgHelpers = +(rows.map(r => Number(r.helpers) || 1).reduce((a, b) => a + b, 0) / rows.length).toFixed(1)
    const totalRej = rows.reduce((s, r) => s + (Number(r.rejections) || 0), 0)
    const totalDrops = rows.reduce((s, r) => s + (Number(r.drops) || 1), 0)
    const rejPct = totalDrops ? +(totalRej / totalDrops * 100).toFixed(1) : 0
    const days = [...new Set(rows.map(r => r.day).filter(Boolean))]
    const driverCount = {}
    rows.forEach(r => { if (r.driver_id) driverCount[r.driver_id] = (driverCount[r.driver_id] || 0) + 1 })
    const topDriver = Object.entries(driverCount).sort((a, b) => b[1] - a[1])[0]?.[0] || ''
    const topDriverPct = topDriver ? Math.round(driverCount[topDriver] / rows.length * 100) : 0
    const cv = avgVol ? stdVol / avgVol : 1
    const confidence = Math.round(Math.min(100, Math.max(0, (rows.length / 20) * 40 + (1 - cv) * 40 + (rejPct < 10 ? 20 : rejPct < 20 ? 10 : 0))))
    const uniqueSites = [...new Set(p.sites.split('|').filter(Boolean))].slice(0, 10)

    return { name: p.name, sites: uniqueSites.join('|'), zone: p.zone, team: p.team, occurrences: rows.length, avgVol, stdVol, avgDrops, avgHelpers, rejPct, days, topDriver, topDriverPct, confidence }
  }).sort((a, b) => b.occurrences - a.occurrences)
}

// ── Driver Scoring ────────────────────────────────────────────────────────────

export function computeDriverScores(data) {
  const map = {}
  const templateAvgs = {}

  data.forEach(r => {
    const key = r.route_name || 'unknown'
    if (!templateAvgs[key]) templateAvgs[key] = { sum: 0, n: 0 }
    templateAvgs[key].sum += Number(r.volume) || 0
    templateAvgs[key].n++
  })

  data.forEach(r => {
    if (!r.driver_id) return
    if (!map[r.driver_id]) map[r.driver_id] = { id: r.driver_id, name: r.driver_name || r.driver_id, days: {}, routes: new Set(), totalVol: 0, rows: [] }
    const d = map[r.driver_id]
    d.rows.push(r)
    d.routes.add(r.route_name)
    d.totalVol += Number(r.volume) || 0
    if (!d.days[r.date]) d.days[r.date] = { drops: 0, successful: 0, rejections: 0, volume: 0 }
    d.days[r.date].drops      += Number(r.drops) || 1
    d.days[r.date].successful += Number(r.successful) || 0
    d.days[r.date].rejections += Number(r.rejections) || 0
    d.days[r.date].volume     += Number(r.volume) || 0
  })

  return Object.values(map).map(drv => {
    const dayArr = Object.values(drv.days)

    const dailyScores = dayArr.map(day => {
      const compRate  = day.drops > 0 ? day.successful / day.drops : 0
      const compScore = +(compRate * 40).toFixed(2)
      const volRatios = drv.rows.map(r => { const ta = templateAvgs[r.route_name]; const avg = ta ? ta.sum / ta.n : Number(r.volume); return avg > 0 ? Number(r.volume) / avg : 1 })
      const avgRatio  = volRatios.length ? volRatios.reduce((a, b) => a + b, 0) / volRatios.length : 1
      const volScore  = +(Math.max(0, Math.min(1, avgRatio)) * 25).toFixed(2)
      const rejRate   = day.drops > 0 ? day.rejections / day.drops : 0
      const rejScore  = +(Math.max(0, Math.min(1, 1 - rejRate * 5)) * 20).toFixed(2)
      return { compScore, volScore, rejScore, total: compScore + volScore + rejScore, compRate, rejRate }
    })

    const avgTotal        = dailyScores.length ? dailyScores.reduce((s, d) => s + d.total, 0) / dailyScores.length : 0
    const variance        = dailyScores.length ? dailyScores.reduce((s, d) => s + (d.total - avgTotal) ** 2, 0) / dailyScores.length : 0
    const consistencyScore = +(Math.max(0, Math.min(1, 1 - Math.sqrt(variance) / 30)) * 15).toFixed(2)
    const finalScore      = +Math.min(100, avgTotal + consistencyScore).toFixed(1)
    const avgCompRate     = dailyScores.length ? +(dailyScores.reduce((s, d) => s + d.compRate, 0) / dailyScores.length * 100).toFixed(1) : 0
    const avgRejRate      = dailyScores.length ? +(dailyScores.reduce((s, d) => s + d.rejRate, 0) / dailyScores.length * 100).toFixed(1) : 0
    const avgCompScore    = +(dailyScores.reduce((s, d) => s + d.compScore, 0) / Math.max(1, dailyScores.length)).toFixed(1)
    const avgVolScore     = +(dailyScores.reduce((s, d) => s + d.volScore, 0) / Math.max(1, dailyScores.length)).toFixed(1)
    const avgRejScore     = +(dailyScores.reduce((s, d) => s + d.rejScore, 0) / Math.max(1, dailyScores.length)).toFixed(1)
    const totalDrops      = dayArr.reduce((s, d) => s + d.drops, 0)
    const totalSuccessful = dayArr.reduce((s, d) => s + d.successful, 0)
    const tier            = finalScore >= 90 ? 'Elite' : finalScore >= 75 ? 'Strong' : finalScore >= 55 ? 'Developing' : 'At Risk'
    const recent          = dailyScores.slice(-10)
    const recentAvg       = recent.length ? recent.reduce((s, d) => s + d.total, 0) / recent.length : 0
    const trend           = recentAvg > avgTotal + 2 ? 'up' : recentAvg < avgTotal - 2 ? 'down' : 'stable'

    return {
      id: drv.id, name: drv.name, finalScore, tier, avgCompRate, avgRejRate,
      daysActive: dayArr.length, totalDrops, totalSuccessful, totalVol: drv.totalVol,
      routeCount: drv.routes.size, trend,
      scores: { completion: avgCompScore, volume: avgVolScore, rejection: avgRejScore, consistency: consistencyScore }
    }
  }).sort((a, b) => b.finalScore - a.finalScore)
}
