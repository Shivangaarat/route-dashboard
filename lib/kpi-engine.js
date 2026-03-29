// lib/kpi-engine.js
// Core KPI calculation engine
// Called after every daily upload to populate daily_tour_metrics and daily_summary

import { neon } from '@neondatabase/serverless'

function db() { return neon(process.env.DATABASE_URL) }

// ── Temperature category mapping ─────────────────────────────────────────────
function getTempCategory(temperature) {
  if (!temperature) return 'Ambient'
  const t = temperature.toUpperCase().trim()
  if (t === 'AMBIENT' || t === '') return 'Ambient'
  return 'Frozen' // Cold Room, Frozen, -20 to Ambient, Chiller etc
}


// ── City name normalisation ───────────────────────────────────────────────────
function normaliseCity(city) {
  if (!city) return ''
  const c = city.trim()
  const map = {
    'abu dhabi': 'Abu Dhabi', 'abudhabi': 'Abu Dhabi', 'abu_dhabi': 'Abu Dhabi',
    'dubai': 'Dubai', 'dxb': 'Dubai', 'dxbjum': 'Dubai', 'dxb jum': 'Dubai',
    'internationalcity': 'Dubai', 'international city': 'Dubai',
    'sharjah': 'Sharjah',
    'ajman': 'Ajman',
    'ras al khaimah': 'Ras Al Khaimah', 'ras al-khaimah': 'Ras Al Khaimah',
    'ras alkhaimah': 'Ras Al Khaimah', 'rasalkhaimah': 'Ras Al Khaimah', 'rak': 'Ras Al Khaimah',
    'fujairah': 'Fujairah',
    'umm al quwain': 'Umm Al Quwain', 'uaq': 'Umm Al Quwain',
    'al ain': 'Al Ain', 'alain': 'Al Ain', 'al-ain': 'Al Ain',
    'hatta': 'Hatta',
  }
  return map[c.toLowerCase()] ||
    c.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ')
}

// ── Analysis category (Overall, Ambient, Frozen, HC) ─────────────────────────
function getAnalysisCategory(team, tempCategory) {
  const t = (team || '').toUpperCase().trim()
  if (t === 'HC') return 'HC'
  if (tempCategory === 'Frozen') return 'NHC Frozen'
  return 'NHC Ambient'
}

// ── Check if route name matches any exclusion pattern ────────────────────────
function isExcluded(routeName, exclusions) {
  if (!routeName) return false
  const name = routeName.toUpperCase()
  return exclusions.some(ex => {
    const pattern = (ex.pattern || '').toUpperCase()
    const type = ex.match_type || 'prefix'
    if (type === 'exact')    return name === pattern
    if (type === 'contains') return name.includes(pattern)
    return name.startsWith(pattern) // prefix (default)
  })
}

// ── HC sub-category: Pharma vs Medlab ────────────────────────────────────────
function isHCPharma(row, pharmaPatterns) {
  const cat = (row.category || '').toUpperCase()
  return pharmaPatterns.some(p => cat.includes(p.toUpperCase()))
}

export async function recalculateDay(dispatchDate) {
  const sql = db()

  // Load config
  const [exclusions, settingsRows, vehicleMaster] = await Promise.all([
    sql`SELECT pattern, match_type FROM route_exclusions WHERE is_active = true`,
    sql`SELECT key, value FROM settings`,
    sql`SELECT vehicle_id, ownership, fleet_type, effective_cbm, is_virtual, team_name FROM vehicle_master`,
  ])

  const settings = {}
  settingsRows.forEach(s => { settings[s.key] = s.value })

  const bulkMinUtil   = parseFloat(settings.bulk_min_util_pct || '80')
  const bulkMinDrops  = parseInt(settings.bulk_min_drops || '3')
  const singleMax     = parseInt(settings.single_drop_max || '2')
  const pharmaPatterns = (settings.pharma_patterns || 'Medicine').split(',').map(s => s.trim())
  const medlabPatterns = (settings.medlab_patterns || 'Medlab').split(',').map(s => s.trim())

  // Build vehicle lookup
  const vehicleMap = {}
  vehicleMaster.forEach(v => { vehicleMap[v.vehicle_id] = v })

  // Load all tasks for this date
  const tasks = await sql`
    SELECT * FROM raw_tasks WHERE dispatch_date = ${dispatchDate}
  `

  if (!tasks.length) return { date: dispatchDate, tours: 0, message: 'No tasks found' }

  // Group tasks by tour
  const tourMap = {}
  tasks.forEach(task => {
    const tourKey = task.planned_tour_name || task.tour_id || 'UNKNOWN'
    if (!tourMap[tourKey]) {
      tourMap[tourKey] = {
        planned_tour_name: tourKey,
        tasks: [],
        location_ids: new Set(),
        vehicle_id: task.vehicle_id,
      }
    }
    tourMap[tourKey].tasks.push(task)
    if (task.location_id) tourMap[tourKey].location_ids.add(task.location_id)
  })

  // Clear existing metrics for this date
  await sql`DELETE FROM daily_tour_metrics WHERE dispatch_date = ${dispatchDate}`
  await sql`DELETE FROM daily_summary       WHERE dispatch_date = ${dispatchDate}`
  await sql`DELETE FROM emirates_daily      WHERE dispatch_date = ${dispatchDate}`

  const tourMetrics = []

  // Calculate per-tour metrics
  for (const [tourName, tour] of Object.entries(tourMap)) {
    const tasks = tour.tasks
    const firstTask = tasks[0]

    // Temperature — use majority temp in tour
    const tempCounts = {}
    tasks.forEach(t => {
      const tc = getTempCategory(t.temperature)
      tempCounts[tc] = (tempCounts[tc] || 0) + 1
    })
    const tempCategory = Object.entries(tempCounts).sort((a,b) => b[1]-a[1])[0][0]

    const team = firstTask.team || 'NHC'
    const analysisCategory = getAnalysisCategory(team, tempCategory)
    const excluded = isExcluded(tourName, exclusions)

    // Vehicle lookup
    const vehicleId = tour.vehicle_id || ''
    const vehicle = vehicleMap[vehicleId] || {}
    const cbmCapacity = vehicle.effective_cbm || null
    const ownership = vehicle.ownership || 'Unknown'
    const fleetType = vehicle.fleet_type || 'Unknown'
    const isVirtualVehicle = vehicle.is_virtual || isExcluded(tourName, exclusions)

    // Drops = unique location IDs
    const uniqueDrops = tour.location_ids.size || 1

    // Volume and weight
    const totalVolume = tasks.reduce((s, t) => s + (parseFloat(t.volume_cbm) || 0), 0)
    const totalWeight = tasks.reduce((s, t) => s + (parseFloat(t.weight_kg) || 0), 0)

    // Volume utilisation
    const volumeUtil = cbmCapacity && cbmCapacity > 0 && !isVirtualVehicle
      ? Math.min(parseFloat((totalVolume / cbmCapacity * 100).toFixed(2)), 999)
      : null

    // Route type and bulk flag
    const routeType = uniqueDrops <= singleMax ? 'Single' : 'Multi'
    const isBulk = routeType === 'Multi' && volumeUtil !== null && volumeUtil >= bulkMinUtil

    // Completions and failures
    const completed = tasks.filter(t => t.is_completed).length
    const failed    = tasks.filter(t => t.is_failed).length
    const rejPct    = tasks.length > 0
      ? parseFloat((failed / tasks.length * 100).toFixed(2))
      : 0

    // HC sub-categories
    const pharmaOrders  = analysisCategory === 'HC' ? tasks.filter(t => isHCPharma(t, pharmaPatterns)).length : 0
    const medlabOrders  = analysisCategory === 'HC' ? tasks.filter(t => medlabPatterns.some(p => (t.category || '').toUpperCase().includes(p.toUpperCase()))).length : 0
    const pharmaFailed  = analysisCategory === 'HC' ? tasks.filter(t => t.is_failed && isHCPharma(t, pharmaPatterns)).length : 0
    const medlabFailed  = analysisCategory === 'HC' ? tasks.filter(t => t.is_failed && medlabPatterns.some(p => (t.category || '').toUpperCase().includes(p.toUpperCase()))).length : 0

    // City (majority city in tour)
    const cityCounts = {}
    tasks.forEach(t => { if (t.city) { const nc = normaliseCity(t.city); cityCounts[nc] = (cityCounts[nc] || 0) + 1 } })
    const city = Object.entries(cityCounts).sort((a,b) => b[1]-a[1])[0]?.[0] || ''

    const metric = {
      dispatch_date:       dispatchDate,
      planned_tour_name:   tourName,
      team,
      temp_category:       tempCategory,
      analysis_category:   analysisCategory,
      city,
      zone:                firstTask.zone || '',
      vehicle_id:          vehicleId,
      vehicle_name:        vehicle.vehicle_name || vehicleId,
      ownership,
      fleet_type:          fleetType,
      cbm_capacity:        cbmCapacity,
      is_virtual_vehicle:  isVirtualVehicle,
      total_orders:        tasks.length,
      unique_drops:        uniqueDrops,
      completed_orders:    completed,
      failed_orders:       failed,
      total_volume_cbm:    parseFloat(totalVolume.toFixed(6)),
      total_weight_kg:     parseFloat(totalWeight.toFixed(3)),
      volume_util_pct:     volumeUtil,
      pallet_util_pct:     null, // pending pallet data
      route_type:          routeType,
      is_bulk:             isBulk,
      is_excluded:         excluded,
      rejection_pct:       rejPct,
      pharma_orders:       pharmaOrders,
      medlab_orders:       medlabOrders,
      pharma_failed:       pharmaFailed,
      medlab_failed:       medlabFailed,
    }

    tourMetrics.push(metric)

    // Insert tour metric
    await sql`
      INSERT INTO daily_tour_metrics (
        dispatch_date, planned_tour_name, team, temp_category, analysis_category,
        city, zone, vehicle_id, vehicle_name, ownership, fleet_type, cbm_capacity,
        is_virtual_vehicle, total_orders, unique_drops, completed_orders, failed_orders,
        total_volume_cbm, total_weight_kg, volume_util_pct, pallet_util_pct,
        route_type, is_bulk, is_excluded, rejection_pct,
        pharma_orders, medlab_orders, pharma_failed, medlab_failed
      ) VALUES (
        ${metric.dispatch_date}, ${metric.planned_tour_name}, ${metric.team},
        ${metric.temp_category}, ${metric.analysis_category}, ${metric.city}, ${metric.zone},
        ${metric.vehicle_id}, ${metric.vehicle_name}, ${metric.ownership}, ${metric.fleet_type},
        ${metric.cbm_capacity}, ${metric.is_virtual_vehicle}, ${metric.total_orders},
        ${metric.unique_drops}, ${metric.completed_orders}, ${metric.failed_orders},
        ${metric.total_volume_cbm}, ${metric.total_weight_kg}, ${metric.volume_util_pct},
        ${metric.pallet_util_pct}, ${metric.route_type}, ${metric.is_bulk}, ${metric.is_excluded},
        ${metric.rejection_pct}, ${metric.pharma_orders}, ${metric.medlab_orders},
        ${metric.pharma_failed}, ${metric.medlab_failed}
      )
      ON CONFLICT (dispatch_date, planned_tour_name) DO UPDATE SET
        total_orders = EXCLUDED.total_orders, unique_drops = EXCLUDED.unique_drops,
        completed_orders = EXCLUDED.completed_orders, failed_orders = EXCLUDED.failed_orders,
        total_volume_cbm = EXCLUDED.total_volume_cbm, volume_util_pct = EXCLUDED.volume_util_pct,
        is_bulk = EXCLUDED.is_bulk, is_excluded = EXCLUDED.is_excluded,
        rejection_pct = EXCLUDED.rejection_pct
    `
  }

  // ── Pre-fetch RD count once for this date (reused across all 4 categories) ──
  const rdCountResult = await sql`
    SELECT COUNT(DISTINCT task_id) as rd_count
    FROM task_attempt_history
    WHERE attempt_date = ${dispatchDate}
    AND attempt_number > 1
  `
  const rdCountForDate = parseInt(rdCountResult[0]?.rd_count || 0)

  // ── Build daily_summary per analysis category ─────────────────────────────
  const categories = ['Overall', 'NHC Ambient', 'NHC Frozen', 'HC']

  for (const cat of categories) {
    const catMetrics = cat === 'Overall'
      ? tourMetrics
      : tourMetrics.filter(m => m.analysis_category === cat)

    const included = catMetrics.filter(m => !m.is_excluded && !m.is_virtual_vehicle)
    const excluded = catMetrics.filter(m => m.is_excluded || m.is_virtual_vehicle)

    if (!catMetrics.length && cat !== 'Overall') continue

    const ownTours    = included.filter(m => m.ownership === 'OWN')
    const dleasedTours = included.filter(m => m.ownership === 'D-LEASED')
    const singleTours = included.filter(m => m.route_type === 'Single')
    const multiTours  = included.filter(m => m.route_type === 'Multi')
    const bulkTours   = included.filter(m => m.is_bulk)

    const totalDrops   = included.reduce((s, m) => s + m.unique_drops, 0)
    const ownDrops     = ownTours.reduce((s, m) => s + m.unique_drops, 0)
    const dleasedDrops = dleasedTours.reduce((s, m) => s + m.unique_drops, 0)
    const totalOrders  = catMetrics.reduce((s, m) => s + m.total_orders, 0)
    const completed    = catMetrics.reduce((s, m) => s + m.completed_orders, 0)
    const failed       = catMetrics.reduce((s, m) => s + m.failed_orders, 0)

    const overallAvgDrops = included.length
      ? parseFloat((totalDrops / included.length).toFixed(2)) : 0
    const avgDropsExclSingle = multiTours.length
      ? parseFloat((multiTours.reduce((s,m) => s+m.unique_drops,0) / multiTours.length).toFixed(2)) : 0
    const ownAvg    = ownTours.length    ? parseFloat((ownDrops / ownTours.length).toFixed(2)) : 0
    const dleasedAvg = dleasedTours.length ? parseFloat((dleasedDrops / dleasedTours.length).toFixed(2)) : 0

    const utilTours = included.filter(m => m.volume_util_pct !== null)
    const avgUtil   = utilTours.length
      ? parseFloat((utilTours.reduce((s,m) => s+m.volume_util_pct,0) / utilTours.length).toFixed(2)) : null

    const rejPct  = totalOrders > 0 ? parseFloat((failed/totalOrders*100).toFixed(2)) : 0
    const firstAttemptSuccess = totalOrders > 0 ? parseFloat((completed/totalOrders*100).toFixed(2)) : 0

    // RD metrics — use pre-fetched count
    const rdCount = rdCountForDate
    const rdPct   = totalOrders > 0 ? parseFloat((rdCount/totalOrders*100).toFixed(2)) : 0

    // HC pharma/medlab RD
    const pharmFailed = catMetrics.reduce((s,m) => s+m.pharma_failed,0)
    const medFailed   = catMetrics.reduce((s,m) => s+m.medlab_failed,0)
    const pharmOrders = catMetrics.reduce((s,m) => s+m.pharma_orders,0)
    const medOrders   = catMetrics.reduce((s,m) => s+m.medlab_orders,0)
    const rdPharmaPct = pharmOrders>0 ? parseFloat((pharmFailed/pharmOrders*100).toFixed(2)) : null
    const rdMedlabPct = medOrders>0   ? parseFloat((medFailed/medOrders*100).toFixed(2))     : null

    await sql`
      INSERT INTO daily_summary (
        dispatch_date, analysis_category,
        total_tours, excluded_tours, included_tours,
        own_vehicles, dleased_vehicles,
        total_drops, own_drops, dleased_drops,
        own_avg_drops, dleased_avg_drops, overall_avg_drops, avg_drops_excl_single,
        single_drop_count, multi_drop_vehicle_count, multi_drop_total,
        single_drop_vehicle_count, single_drop_total,
        total_volume_cbm, avg_volume_util_pct,
        bulk_route_count, bulk_route_pct,
        total_orders, completed_orders, failed_orders,
        daily_rejection_pct, first_attempt_success_pct,
        rd_count, rd_pct, rd_pharma_pct, rd_medlab_pct
      ) VALUES (
        ${dispatchDate}, ${cat},
        ${catMetrics.length}, ${excluded.length}, ${included.length},
        ${ownTours.length}, ${dleasedTours.length},
        ${totalDrops}, ${ownDrops}, ${dleasedDrops},
        ${ownAvg}, ${dleasedAvg}, ${overallAvgDrops}, ${avgDropsExclSingle},
        ${singleTours.length}, ${multiTours.length},
        ${multiTours.reduce((s,m)=>s+m.unique_drops,0)},
        ${singleTours.length}, ${singleTours.reduce((s,m)=>s+m.unique_drops,0)},
        ${parseFloat(catMetrics.reduce((s,m)=>s+m.total_volume_cbm,0).toFixed(3))},
        ${avgUtil},
        ${bulkTours.length},
        ${included.length > 0 ? parseFloat((bulkTours.length/included.length*100).toFixed(2)) : 0},
        ${totalOrders}, ${completed}, ${failed},
        ${rejPct}, ${firstAttemptSuccess},
        ${rdCount}, ${rdPct}, ${rdPharmaPct}, ${rdMedlabPct}
      )
      ON CONFLICT (dispatch_date, analysis_category) DO UPDATE SET
        total_tours = EXCLUDED.total_tours, included_tours = EXCLUDED.included_tours,
        total_drops = EXCLUDED.total_drops, overall_avg_drops = EXCLUDED.overall_avg_drops,
        daily_rejection_pct = EXCLUDED.daily_rejection_pct,
        bulk_route_count = EXCLUDED.bulk_route_count,
        rd_pct = EXCLUDED.rd_pct
    `
  }

  // ── Emirates breakdown ────────────────────────────────────────────────────
  const cities = [...new Set(tourMetrics.map(m => normaliseCity(m.city || '')).filter(Boolean))]
  for (const city of cities) {
    for (const cat of ['Overall', 'NHC Ambient', 'NHC Frozen', 'HC']) {
      const cityMetrics = (cat === 'Overall'
        ? tourMetrics
        : tourMetrics.filter(m => m.analysis_category === cat)
      ).filter(m => normaliseCity(m.city || '') === city)

      if (!cityMetrics.length) continue

      const orders    = cityMetrics.reduce((s,m) => s+m.total_orders, 0)
      const drops     = cityMetrics.reduce((s,m) => s+m.unique_drops, 0)
      const completed = cityMetrics.reduce((s,m) => s+m.completed_orders, 0)
      const failed    = cityMetrics.reduce((s,m) => s+m.failed_orders, 0)
      const volume    = parseFloat(cityMetrics.reduce((s,m) => s+m.total_volume_cbm, 0).toFixed(3))
      const rejPct    = orders > 0 ? parseFloat((failed/orders*100).toFixed(2)) : 0

      await sql`
        INSERT INTO emirates_daily (dispatch_date, analysis_category, city,
          total_orders, total_drops, completed_orders, failed_orders, total_volume_cbm, rejection_pct)
        VALUES (${dispatchDate}, ${cat}, ${city},
          ${orders}, ${drops}, ${completed}, ${failed}, ${volume}, ${rejPct})
        ON CONFLICT (dispatch_date, analysis_category, city) DO UPDATE SET
          total_orders = EXCLUDED.total_orders, total_drops = EXCLUDED.total_drops,
          rejection_pct = EXCLUDED.rejection_pct
      `
    }
  }

  return {
    date:          dispatchDate,
    tours:         tourMetrics.length,
    included:      tourMetrics.filter(m => !m.is_excluded).length,
    excluded:      tourMetrics.filter(m => m.is_excluded).length,
    bulk:          tourMetrics.filter(m => m.is_bulk).length,
    total_orders:  tourMetrics.reduce((s,m) => s+m.total_orders, 0),
    categories:    categories,
  }
}
