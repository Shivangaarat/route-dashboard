// lib/kpi-engine.js — v2 bulk SQL edition
// All calculations done in SQL/JS aggregation — no per-tour round trips

import { neon } from '@neondatabase/serverless'

function db() { return neon(process.env.DATABASE_URL) }

function getTempCategory(t) {
  if (!t) return 'Ambient'
  return t.toUpperCase().trim() === 'AMBIENT' ? 'Ambient' : 'Frozen'
}

function getAnalysisCategory(team, tempCat) {
  if ((team||'').toUpperCase().trim() === 'HC') return 'HC'
  return tempCat === 'Frozen' ? 'NHC Frozen' : 'NHC Ambient'
}

function normaliseCity(city) {
  if (!city) return ''
  const c = city.trim()
  const map = {
    'abu dhabi':'Abu Dhabi','abudhabi':'Abu Dhabi','abu_dhabi':'Abu Dhabi',
    'dubai':'Dubai','dxb':'Dubai','dxbjum':'Dubai','internationalcity':'Dubai','international city':'Dubai',
    'sharjah':'Sharjah','ajman':'Ajman',
    'ras al khaimah':'Ras Al Khaimah','ras al-khaimah':'Ras Al Khaimah','rasalkhaimah':'Ras Al Khaimah','rak':'Ras Al Khaimah',
    'fujairah':'Fujairah','umm al quwain':'Umm Al Quwain','uaq':'Umm Al Quwain',
    'al ain':'Al Ain','alain':'Al Ain','hatta':'Hatta',
  }
  return map[c.toLowerCase()] || c.split(' ').map(w=>w.charAt(0).toUpperCase()+w.slice(1).toLowerCase()).join(' ')
}

function isExcluded(routeName, exclusions) {
  if (!routeName) return false
  const name = routeName.toUpperCase()
  return exclusions.some(ex => {
    const p = (ex.pattern||'').toUpperCase()
    const t = ex.match_type || 'prefix'
    if (t === 'exact')    return name === p
    if (t === 'contains') return name.includes(p)
    return name.startsWith(p)
  })
}

export async function recalculateDay(dispatchDate) {
  const sql = db()

  // ── Load config in parallel ─────────────────────────────────────────────────
  const [exclusions, settingsRows, vehicleMaster, tasks, rdCount] = await Promise.all([
    sql`SELECT pattern, match_type FROM route_exclusions WHERE is_active = true`,
    sql`SELECT key, value FROM settings`,
    sql`SELECT vehicle_id, ownership, fleet_type, effective_cbm, is_virtual FROM vehicle_master`,
    sql`SELECT task_id, planned_tour_name, tour_id, team, temperature, temp_category,
               zone, city, vehicle_id, location_id, volume_cbm, weight_kg,
               is_completed, is_failed, category, organisation, division, operating_unit
        FROM raw_tasks WHERE dispatch_date = ${dispatchDate}`,
    sql`SELECT COUNT(DISTINCT task_id) as cnt FROM task_attempt_history
        WHERE attempt_date = ${dispatchDate} AND attempt_number > 1`,
  ])

  if (!tasks.length) return { date: dispatchDate, tours: 0, message: 'No tasks found' }

  const settings = {}
  settingsRows.forEach(s => { settings[s.key] = s.value })
  const bulkMinUtil  = parseFloat(settings.bulk_min_util_pct || '80')
  const singleMax    = parseInt(settings.single_drop_max || '2')
  const pharmaPatterns = (settings.pharma_patterns || 'Medicine').split(',').map(s=>s.trim().toUpperCase())
  const medlabPatterns = (settings.medlab_patterns || 'Medlab').split(',').map(s=>s.trim().toUpperCase())
  const rdCountForDate = parseInt(rdCount[0]?.cnt || 0)

  const vehicleMap = {}
  vehicleMaster.forEach(v => { vehicleMap[v.vehicle_id] = v })

  // ── Group tasks by tour in JS (no extra DB calls) ──────────────────────────
  const tourMap = {}
  tasks.forEach(task => {
    const key = task.planned_tour_name || task.tour_id || 'UNKNOWN'
    if (!tourMap[key]) tourMap[key] = { name: key, tasks: [], locations: new Set(), vehicle_id: task.vehicle_id }
    tourMap[key].tasks.push(task)
    if (task.location_id) tourMap[key].locations.add(task.location_id)
  })

  // ── Calculate all tour metrics in JS ───────────────────────────────────────
  const tourMetrics = []
  for (const [tourName, tour] of Object.entries(tourMap)) {
    const t = tour.tasks
    const first = t[0]

    // Majority temperature
    const tc = {}
    t.forEach(r => { const cat = getTempCategory(r.temperature); tc[cat] = (tc[cat]||0)+1 })
    const tempCategory = Object.entries(tc).sort((a,b)=>b[1]-a[1])[0][0]

    const team = first.team || 'NHC'
    const analysisCategory = getAnalysisCategory(team, tempCategory)
    const excluded = isExcluded(tourName, exclusions)
    const vehicle = vehicleMap[tour.vehicle_id] || {}
    const cbmCap = vehicle.effective_cbm || null
    const isVirtual = vehicle.is_virtual || false
    const ownership = vehicle.ownership || 'Unknown'
    const uniqueDrops = tour.locations.size || 1
    const totalVol = t.reduce((s,r)=>s+(parseFloat(r.volume_cbm)||0),0)
    const totalWt  = t.reduce((s,r)=>s+(parseFloat(r.weight_kg)||0),0)
    const volUtil  = cbmCap && cbmCap>0 && !isVirtual
      ? Math.min(parseFloat((totalVol/cbmCap*100).toFixed(2)),999) : null
    const routeType = uniqueDrops <= singleMax ? 'Single' : 'Multi'
    const isBulk = routeType==='Multi' && volUtil!==null && volUtil>=bulkMinUtil
    const completed = t.filter(r=>r.is_completed).length
    const failed    = t.filter(r=>r.is_failed).length
    const rejPct    = t.length>0 ? parseFloat((failed/t.length*100).toFixed(2)) : 0

    // HC pharma/medlab
    const pharmaOrders = analysisCategory==='HC' ? t.filter(r=>pharmaPatterns.some(p=>(r.category||'').toUpperCase().includes(p))).length : 0
    const medlabOrders = analysisCategory==='HC' ? t.filter(r=>medlabPatterns.some(p=>(r.category||'').toUpperCase().includes(p))).length : 0
    const pharmaFailed = analysisCategory==='HC' ? t.filter(r=>r.is_failed&&pharmaPatterns.some(p=>(r.category||'').toUpperCase().includes(p))).length : 0
    const medlabFailed = analysisCategory==='HC' ? t.filter(r=>r.is_failed&&medlabPatterns.some(p=>(r.category||'').toUpperCase().includes(p))).length : 0

    // Majority city
    const cc = {}
    t.forEach(r=>{ if(r.city){const nc=normaliseCity(r.city);cc[nc]=(cc[nc]||0)+1} })
    const city = Object.entries(cc).sort((a,b)=>b[1]-a[1])[0]?.[0] || ''

    tourMetrics.push({
      dispatch_date: dispatchDate, planned_tour_name: tourName,
      team, temp_category: tempCategory, analysis_category: analysisCategory,
      city,
      operating_unit: [...new Set(t.map(r=>r.operating_unit||'').filter(Boolean))].join(',') || '',
      zone: first.zone||'', vehicle_id: tour.vehicle_id||'',
      vehicle_name: vehicle.vehicle_name||'', ownership, fleet_type: vehicle.fleet_type||'',
      cbm_capacity: cbmCap, is_virtual_vehicle: isVirtual,
      total_orders: t.length, unique_drops: uniqueDrops,
      completed_orders: completed, failed_orders: failed,
      total_volume_cbm: parseFloat(totalVol.toFixed(6)),
      total_weight_kg: parseFloat(totalWt.toFixed(3)),
      volume_util_pct: volUtil, pallet_util_pct: null,
      route_type: routeType, is_bulk: isBulk, is_excluded: excluded,
      rejection_pct: rejPct,
      pharma_orders: pharmaOrders, medlab_orders: medlabOrders,
      pharma_failed: pharmaFailed, medlab_failed: medlabFailed,
    })
  }

  // ── Clear old data for this date ───────────────────────────────────────────
  await Promise.all([
    sql`DELETE FROM daily_tour_metrics WHERE dispatch_date = ${dispatchDate}`,
    sql`DELETE FROM daily_summary       WHERE dispatch_date = ${dispatchDate}`,
    sql`DELETE FROM emirates_daily      WHERE dispatch_date = ${dispatchDate}`,
  ])

  // ── Batch insert tour metrics (10 at a time) ───────────────────────────────
  for (let i=0; i<tourMetrics.length; i+=10) {
    const batch = tourMetrics.slice(i, i+10)
    await Promise.all(batch.map(m => sql`
      INSERT INTO daily_tour_metrics (
        dispatch_date, planned_tour_name, team, temp_category, analysis_category,
        city, operating_unit, zone, vehicle_id, vehicle_name, ownership, fleet_type, cbm_capacity,
        is_virtual_vehicle, total_orders, unique_drops, completed_orders, failed_orders,
        total_volume_cbm, total_weight_kg, volume_util_pct, pallet_util_pct,
        route_type, is_bulk, is_excluded, rejection_pct,
        pharma_orders, medlab_orders, pharma_failed, medlab_failed
      ) VALUES (
        ${m.dispatch_date}, ${m.planned_tour_name}, ${m.team}, ${m.temp_category},
        ${m.analysis_category}, ${m.city}, ${m.operating_unit}, ${m.zone}, ${m.vehicle_id}, ${m.vehicle_name},
        ${m.ownership}, ${m.fleet_type}, ${m.cbm_capacity}, ${m.is_virtual_vehicle},
        ${m.total_orders}, ${m.unique_drops}, ${m.completed_orders}, ${m.failed_orders},
        ${m.total_volume_cbm}, ${m.total_weight_kg}, ${m.volume_util_pct}, ${m.pallet_util_pct},
        ${m.route_type}, ${m.is_bulk}, ${m.is_excluded}, ${m.rejection_pct},
        ${m.pharma_orders}, ${m.medlab_orders}, ${m.pharma_failed}, ${m.medlab_failed}
      )
      ON CONFLICT (dispatch_date, planned_tour_name) DO UPDATE SET
        total_orders=EXCLUDED.total_orders, unique_drops=EXCLUDED.unique_drops,
        completed_orders=EXCLUDED.completed_orders, failed_orders=EXCLUDED.failed_orders,
        total_volume_cbm=EXCLUDED.total_volume_cbm, volume_util_pct=EXCLUDED.volume_util_pct,
        is_bulk=EXCLUDED.is_bulk, is_excluded=EXCLUDED.is_excluded,
        rejection_pct=EXCLUDED.rejection_pct
    `))
  }

  // ── Build daily_summary per category ──────────────────────────────────────
  const categories = ['Overall','NHC Ambient','NHC Frozen','HC']
  const summaryInserts = []

  for (const cat of categories) {
    const cm = cat==='Overall' ? tourMetrics : tourMetrics.filter(m=>m.analysis_category===cat)
    const included = cm.filter(m=>!m.is_excluded && !m.is_virtual_vehicle)
    if (!cm.length && cat!=='Overall') continue

    const own     = included.filter(m=>m.ownership==='OWN')
    const dleased = included.filter(m=>m.ownership==='D-LEASED')
    const single  = included.filter(m=>m.route_type==='Single')
    const multi   = included.filter(m=>m.route_type==='Multi')
    const bulk    = included.filter(m=>m.is_bulk)

    const totalDrops   = included.reduce((s,m)=>s+m.unique_drops,0)
    const ownDrops     = own.reduce((s,m)=>s+m.unique_drops,0)
    const dlDrops      = dleased.reduce((s,m)=>s+m.unique_drops,0)
    const totalOrders  = cm.reduce((s,m)=>s+m.total_orders,0)
    const completed    = cm.reduce((s,m)=>s+m.completed_orders,0)
    const failed       = cm.reduce((s,m)=>s+m.failed_orders,0)
    const totalVol     = parseFloat(cm.reduce((s,m)=>s+m.total_volume_cbm,0).toFixed(3))

    const overallAvg   = included.length ? parseFloat((totalDrops/included.length).toFixed(2)) : 0
    const multiAvg     = multi.length ? parseFloat((multi.reduce((s,m)=>s+m.unique_drops,0)/multi.length).toFixed(2)) : 0
    const ownAvg       = own.length ? parseFloat((ownDrops/own.length).toFixed(2)) : 0
    const dlAvg        = dleased.length ? parseFloat((dlDrops/dleased.length).toFixed(2)) : 0
    const utilTours    = included.filter(m=>m.volume_util_pct!==null)
    const avgUtil      = utilTours.length ? parseFloat((utilTours.reduce((s,m)=>s+m.volume_util_pct,0)/utilTours.length).toFixed(2)) : null
    const rejPct       = totalOrders>0 ? parseFloat((failed/totalOrders*100).toFixed(2)) : 0
    const firstAttempt = totalOrders>0 ? parseFloat((completed/totalOrders*100).toFixed(2)) : 0
    const rdPct        = totalOrders>0 ? parseFloat((rdCountForDate/totalOrders*100).toFixed(2)) : 0
    const pharmOrders  = cm.reduce((s,m)=>s+m.pharma_orders,0)
    const medOrders    = cm.reduce((s,m)=>s+m.medlab_orders,0)
    const pharmFailed  = cm.reduce((s,m)=>s+m.pharma_failed,0)
    const medFailed    = cm.reduce((s,m)=>s+m.medlab_failed,0)
    const rdPharmaPct  = pharmOrders>0 ? parseFloat((pharmFailed/pharmOrders*100).toFixed(2)) : null
    const rdMedlabPct  = medOrders>0   ? parseFloat((medFailed/medOrders*100).toFixed(2))     : null

    summaryInserts.push(sql`
      INSERT INTO daily_summary (
        dispatch_date, analysis_category,
        total_tours, excluded_tours, included_tours,
        own_vehicles, dleased_vehicles, total_drops, own_drops, dleased_drops,
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
        ${cm.length}, ${cm.filter(m=>m.is_excluded||m.is_virtual_vehicle).length}, ${included.length},
        ${own.length}, ${dleased.length}, ${totalDrops}, ${ownDrops}, ${dlDrops},
        ${ownAvg}, ${dlAvg}, ${overallAvg}, ${multiAvg},
        ${single.length}, ${multi.length}, ${multi.reduce((s,m)=>s+m.unique_drops,0)},
        ${single.length}, ${single.reduce((s,m)=>s+m.unique_drops,0)},
        ${totalVol}, ${avgUtil},
        ${bulk.length}, ${included.length>0?parseFloat((bulk.length/included.length*100).toFixed(2)):0},
        ${totalOrders}, ${completed}, ${failed},
        ${rejPct}, ${firstAttempt},
        ${rdCountForDate}, ${rdPct}, ${rdPharmaPct}, ${rdMedlabPct}
      )
      ON CONFLICT (dispatch_date, analysis_category) DO UPDATE SET
        total_tours=EXCLUDED.total_tours, included_tours=EXCLUDED.included_tours,
        total_drops=EXCLUDED.total_drops, overall_avg_drops=EXCLUDED.overall_avg_drops,
        daily_rejection_pct=EXCLUDED.daily_rejection_pct,
        bulk_route_count=EXCLUDED.bulk_route_count, rd_pct=EXCLUDED.rd_pct
    `)
  }
  await Promise.all(summaryInserts)

  // ── Emirates breakdown (batch parallel) ───────────────────────────────────
  const citySet = [...new Set(tourMetrics.map(m=>normaliseCity(m.city||'')).filter(Boolean))]
  const emiratesInserts = []

  for (const city of citySet) {
    for (const cat of ['Overall','NHC Ambient','NHC Frozen','HC']) {
      const cm = (cat==='Overall' ? tourMetrics : tourMetrics.filter(m=>m.analysis_category===cat))
        .filter(m=>normaliseCity(m.city||'')===city)
      if (!cm.length) continue
      const orders=cm.reduce((s,m)=>s+m.total_orders,0)
      const drops=cm.reduce((s,m)=>s+m.unique_drops,0)
      const comp=cm.reduce((s,m)=>s+m.completed_orders,0)
      const fail=cm.reduce((s,m)=>s+m.failed_orders,0)
      const vol=parseFloat(cm.reduce((s,m)=>s+m.total_volume_cbm,0).toFixed(3))
      const rej=orders>0?parseFloat((fail/orders*100).toFixed(2)):0
      emiratesInserts.push(sql`
        INSERT INTO emirates_daily (dispatch_date,analysis_category,city,
          total_orders,total_drops,completed_orders,failed_orders,total_volume_cbm,rejection_pct)
        VALUES (${dispatchDate},${cat},${city},${orders},${drops},${comp},${fail},${vol},${rej})
        ON CONFLICT (dispatch_date,analysis_category,city) DO UPDATE SET
          total_orders=EXCLUDED.total_orders, total_drops=EXCLUDED.total_drops,
          rejection_pct=EXCLUDED.rejection_pct
      `)
    }
  }

  // Run emirates inserts in batches of 20
  for (let i=0; i<emiratesInserts.length; i+=20) {
    await Promise.all(emiratesInserts.slice(i, i+20))
  }

  return {
    date: dispatchDate,
    tours: tourMetrics.length,
    included: tourMetrics.filter(m=>!m.is_excluded).length,
    excluded: tourMetrics.filter(m=>m.is_excluded).length,
    bulk: tourMetrics.filter(m=>m.is_bulk).length,
    total_orders: tourMetrics.reduce((s,m)=>s+m.total_orders,0),
  }
}
