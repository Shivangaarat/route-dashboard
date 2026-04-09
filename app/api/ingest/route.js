// app/api/ingest/route.js
// Uses single bulk INSERT for all rows — handles large multi-day files

import { neon } from '@neondatabase/serverless'
import { recalculateDay } from '../../../lib/kpi-engine.js'
import * as XLSX from 'xlsx'

export const runtime = 'nodejs'
export const maxDuration = 60

export const config = {
  api: {
    bodyParser: {
      sizeLimit: '50mb',
    },
  },
}

function db() { return neon(process.env.DATABASE_URL) }

function getTempCategory(t) {
  if (!t) return 'Ambient'
  return t.toUpperCase().trim() === 'AMBIENT' ? 'Ambient' : 'Frozen'
}

function normaliseCity(raw) {
  if (!raw) return ''
  const c = raw.trim()
  const map = {
    'abu dhabi':'Abu Dhabi','abudhabi':'Abu Dhabi',
    'dubai':'Dubai','dxb':'Dubai','dxbjum':'Dubai','internationalcity':'Dubai','international city':'Dubai',
    'sharjah':'Sharjah','ajman':'Ajman',
    'ras al khaimah':'Ras Al Khaimah','ras al-khaimah':'Ras Al Khaimah','rasalkhaimah':'Ras Al Khaimah','rak':'Ras Al Khaimah',
    'fujairah':'Fujairah','umm al quwain':'Umm Al Quwain','uaq':'Umm Al Quwain',
    'al ain':'Al Ain','alain':'Al Ain','hatta':'Hatta',
  }
  return map[c.toLowerCase()] || c.split(' ').map(w=>w.charAt(0).toUpperCase()+w.slice(1).toLowerCase()).join(' ')
}

function parseDate(v) {
  if (!v) return null
  if (v instanceof Date) return v.toISOString().split('T')[0]
  const s = String(v).trim()
  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/)
  if (m) return `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`
  return s.split(' ')[0]
}

export async function POST(request) {
  try {
    const formData = await request.formData()
    const file = formData.get('file')
    const source = formData.get('source') || 'manual'
    if (!file) return Response.json({ error: 'No file provided' }, { status: 400 })

    const buffer = Buffer.from(await file.arrayBuffer())
    const wb = XLSX.read(buffer, { type: 'buffer', cellDates: true })
    const ws = wb.Sheets[wb.SheetNames[0]]
    const raw = XLSX.utils.sheet_to_json(ws, { defval: '' })
    if (!raw.length) return Response.json({ error: 'No data rows found' }, { status: 400 })

    // Parse all rows
    const tasks = []
    for (const rawRow of raw) {
      const row = {}
      Object.keys(rawRow).forEach(k => { row[k.replace(/^\uFEFF/,'').trim()] = rawRow[k] })
      const get = (...keys) => { for (const k of keys) { const v = row[k]; if (v !== undefined && v !== null && v !== '') return v } return '' }

      const dateStr = parseDate(get('DISPATCH DATE','DELIVERY DATE'))
      if (!dateStr || dateStr.length < 10) continue
      // Handle BOM character on TASK ID column (\uFEFF prefix)
      const taskId = String(get('TASK ID', '\uFEFFTASK ID', 'ALTERNATE ID') || '').trim()
      if (!taskId) continue

      // Support both Locus-style (TASK STATUS) and AKI-style (Final Status / Status)
      const finalStatus = String(get('Final Status') || '').trim()
      const rawStatus   = String(get('TASK STATUS','TRANSACTION STATUS','Status') || '').toUpperCase()
      const taskStatus  = finalStatus ? finalStatus.toUpperCase() : rawStatus
      const temperature = String(get('TEMPERATURE','Temperature') || '')

      // Extract base task ID — strip Locus re-attempt suffix -YYYY-MM-DD-N
      const baseTaskId = taskId.replace(/-\d{4}-\d{2}-\d{2}-\d+$/, '')

      const isCompleted = taskStatus === 'COMPLETED' || taskStatus === 'DELIVERED'
      const isFailed    = taskStatus === 'REJECTION' ||
                          ['FAILED','CANCELLED','REJECTED','NOT DELIVERED','UNDELIVERED','HOLD','PARTIAL'].some(s=>taskStatus.includes(s))

      tasks.push({
        task_id:           taskId,
        base_task_id:      baseTaskId,
        tour_id:           String(get('TOUR ID') || '').trim(),
        planned_tour_name: String(get('PLANNED TOUR NAME') || get('TOUR ID') || '').trim(),
        dispatch_date:     dateStr,
        team:              String(get('TEAM NAME') || '').trim(),
        temperature,
        temp_category:     getTempCategory(temperature),
        zone:              String(get('ZONE') || '').trim(),
        city:              normaliseCity(String(get('CITY') || '')),
        rider_id:          String(get('RIDER ID') || '').trim(),
        rider_name:        String(get('RIDER NAME') || '').trim(),
        vehicle_id:        String(get('VEHICLE ID') || '').trim(),
        vehicle_name:      String(get('VEHICLE NAME') || '').trim(),
        vehicle_reg:       String(get('VEHICLE REGISTRATION NUMBER', "Tractor's Registration") || '').trim(),
        location_id:       String(get('LOCATION ID') || '').trim(),
        location_name:     String(get('LOCATION NAME') || '').trim(),
        customer_name:     String(get('CUSTOMER NAME') || '').trim(),
        volume_cbm:        parseFloat(get('VOLUME') || 0) || 0,
        weight_kg:         parseFloat(get('WEIGHT') || 0) || 0,
        task_status:       taskStatus,
        is_completed:      isCompleted,
        is_failed:         isFailed,
        root_cause:        String(get('REASON','ROOT CAUSE','Cancel reason') || '').trim().toUpperCase(),
        operating_unit:    String(get('OPERATING UNIT','operating_unit') || '').trim(),
        organisation:      String(get('ORGANISATION','Organization') || '').trim(),
        division:          String(get('DIVISION') || '').trim(),
        internal_org:      String(get('INTERNAL ORG','Internal Order') || '').trim(),
        category:          String(get('CATEGORY') || '').trim(),
        invoice_value:     parseFloat(get('INVOICE VALUE') || 0) || null,
        upload_source:     source,
      })
    }

    if (!tasks.length) return Response.json({ error: 'No valid rows after parsing.' }, { status: 400 })

    // Deduplicate — if same task_id + dispatch_date appears twice in the Excel file,
    // keep the last occurrence (prevents "ON CONFLICT DO UPDATE affected row twice" error)
    const seen = new Map()
    for (const t of tasks) {
      seen.set(`${t.task_id}||${t.dispatch_date}`, t)
    }
    const dedupedTasks = [...seen.values()]

    const sql = db()
    const dates = [...new Set(dedupedTasks.map(t => t.dispatch_date))].sort()

    // ── True bulk INSERT using unnest ─────────────────────────────────────────
    // Pass arrays of values — Postgres inserts all rows in one query
    await sql`
      INSERT INTO raw_tasks (
        task_id, base_task_id, tour_id, planned_tour_name, dispatch_date, team,
        temperature, temp_category, zone, city,
        rider_id, rider_name, vehicle_id, vehicle_name, vehicle_reg,
        location_id, location_name, customer_name,
        volume_cbm, weight_kg, task_status, is_completed, is_failed,
        root_cause, operating_unit, organisation, division, internal_org, category,
        invoice_value, upload_source
      )
      SELECT * FROM unnest(
        ${dedupedTasks.map(t=>t.task_id)}::text[],
        ${dedupedTasks.map(t=>t.base_task_id)}::text[],
        ${dedupedTasks.map(t=>t.tour_id)}::text[],
        ${dedupedTasks.map(t=>t.planned_tour_name)}::text[],
        ${dedupedTasks.map(t=>t.dispatch_date)}::date[],
        ${dedupedTasks.map(t=>t.team)}::text[],
        ${dedupedTasks.map(t=>t.temperature)}::text[],
        ${dedupedTasks.map(t=>t.temp_category)}::text[],
        ${dedupedTasks.map(t=>t.zone)}::text[],
        ${dedupedTasks.map(t=>t.city)}::text[],
        ${dedupedTasks.map(t=>t.rider_id)}::text[],
        ${dedupedTasks.map(t=>t.rider_name)}::text[],
        ${dedupedTasks.map(t=>t.vehicle_id)}::text[],
        ${dedupedTasks.map(t=>t.vehicle_name)}::text[],
        ${dedupedTasks.map(t=>t.vehicle_reg)}::text[],
        ${dedupedTasks.map(t=>t.location_id)}::text[],
        ${dedupedTasks.map(t=>t.location_name)}::text[],
        ${dedupedTasks.map(t=>t.customer_name)}::text[],
        ${dedupedTasks.map(t=>t.volume_cbm)}::numeric[],
        ${dedupedTasks.map(t=>t.weight_kg)}::numeric[],
        ${dedupedTasks.map(t=>t.task_status)}::text[],
        ${dedupedTasks.map(t=>t.is_completed)}::boolean[],
        ${dedupedTasks.map(t=>t.is_failed)}::boolean[],
        ${dedupedTasks.map(t=>t.root_cause)}::text[],
        ${dedupedTasks.map(t=>t.operating_unit)}::text[],
        ${dedupedTasks.map(t=>t.organisation)}::text[],
        ${dedupedTasks.map(t=>t.division)}::text[],
        ${dedupedTasks.map(t=>t.internal_org)}::text[],
        ${dedupedTasks.map(t=>t.category)}::text[],
        ${dedupedTasks.map(t=>t.invoice_value)}::numeric[],
        ${dedupedTasks.map(t=>t.upload_source)}::text[]
      ) AS t(task_id,base_task_id,tour_id,planned_tour_name,dispatch_date,team,
             temperature,temp_category,zone,city,
             rider_id,rider_name,vehicle_id,vehicle_name,vehicle_reg,
             location_id,location_name,customer_name,
             volume_cbm,weight_kg,task_status,is_completed,is_failed,
             root_cause,operating_unit,organisation,division,internal_org,category,
             invoice_value,upload_source)
      ON CONFLICT (task_id, dispatch_date) DO UPDATE SET
        task_status      = EXCLUDED.task_status,
        is_completed     = EXCLUDED.is_completed,
        is_failed        = EXCLUDED.is_failed,
        root_cause       = EXCLUDED.root_cause,
        operating_unit   = EXCLUDED.operating_unit,
        upload_source    = EXCLUDED.upload_source,
        customer_name    = EXCLUDED.customer_name,
        location_name    = EXCLUDED.location_name,
        city             = EXCLUDED.city,
        zone             = EXCLUDED.zone,
        organisation     = EXCLUDED.organisation,
        vehicle_reg      = EXCLUDED.vehicle_reg,
        rider_name       = EXCLUDED.rider_name,
        planned_tour_name = EXCLUDED.planned_tour_name,
        category         = EXCLUDED.category,
        invoice_value    = EXCLUDED.invoice_value
    `

    // ── For single-date uploads only, run KPI immediately ────────────────────
    const kpiResults = []
    const skipKPI = dates.length > 1
    if (!skipKPI) {
      try {
        const result = await recalculateDay(dates[0])
        kpiResults.push(result)
      } catch(e) {
        kpiResults.push({ date: dates[0], error: e.message })
      }
    }

    return Response.json({
      status:      'success',
      rows_parsed: tasks.length,
      rows_saved:  dedupedTasks.length,
      dates,
      kpi_results: kpiResults,
      kpi_skipped: skipKPI,
      kpi_message: skipKPI
        ? `${tasks.length} rows saved across ${dates.length} dates. Click Recalculate for each date below.`
        : null,
    })

  } catch (err) {
    console.error('[ingest] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}
