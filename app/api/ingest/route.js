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
      const taskId = String(get('TASK ID') || '').trim()
      if (!taskId) continue

      const taskStatus = String(get('TASK STATUS','TRANSACTION STATUS') || '').toUpperCase()
      const temperature = String(get('TEMPERATURE') || '')

      // Extract base task ID — strip Locus re-attempt suffix -YYYY-MM-DD-N
      const baseTaskId = taskId.replace(/-\d{4}-\d{2}-\d{2}-\d+$/, '')

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
        vehicle_reg:       String(get('VEHICLE REGISTRATION NUMBER') || '').trim(),
        location_id:       String(get('LOCATION ID') || '').trim(),
        location_name:     String(get('LOCATION NAME') || '').trim(),
        customer_name:     String(get('CUSTOMER NAME') || '').trim(),
        volume_cbm:        parseFloat(get('VOLUME') || 0) || 0,
        weight_kg:         parseFloat(get('WEIGHT') || 0) || 0,
        task_status:       taskStatus,
        is_completed:      taskStatus === 'COMPLETED',
        is_failed:         ['FAILED','CANCELLED','REJECTED','NOT DELIVERED','UNDELIVERED'].some(s=>taskStatus.includes(s)),
        root_cause:        String(get('ROOT CAUSE','Cancel reason') || '').trim(),
        operating_unit:    String(get('OPERATING UNIT','operating_unit') || '').trim(),
        organisation:      String(get('ORGANISATION') || '').trim(),
        division:          String(get('DIVISION') || '').trim(),
        internal_org:      String(get('INTERNAL ORG') || '').trim(),
        category:          String(get('CATEGORY') || '').trim(),
        invoice_value:     parseFloat(get('INVOICE VALUE') || 0) || null,
        upload_source:     source,
      })
    }

    if (!tasks.length) return Response.json({ error: 'No valid rows after parsing.' }, { status: 400 })

    const sql = db()
    const dates = [...new Set(tasks.map(t => t.dispatch_date))].sort()

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
        ${tasks.map(t=>t.task_id)}::text[],
        ${tasks.map(t=>t.base_task_id)}::text[],
        ${tasks.map(t=>t.tour_id)}::text[],
        ${tasks.map(t=>t.planned_tour_name)}::text[],
        ${tasks.map(t=>t.dispatch_date)}::date[],
        ${tasks.map(t=>t.team)}::text[],
        ${tasks.map(t=>t.temperature)}::text[],
        ${tasks.map(t=>t.temp_category)}::text[],
        ${tasks.map(t=>t.zone)}::text[],
        ${tasks.map(t=>t.city)}::text[],
        ${tasks.map(t=>t.rider_id)}::text[],
        ${tasks.map(t=>t.rider_name)}::text[],
        ${tasks.map(t=>t.vehicle_id)}::text[],
        ${tasks.map(t=>t.vehicle_name)}::text[],
        ${tasks.map(t=>t.vehicle_reg)}::text[],
        ${tasks.map(t=>t.location_id)}::text[],
        ${tasks.map(t=>t.location_name)}::text[],
        ${tasks.map(t=>t.customer_name)}::text[],
        ${tasks.map(t=>t.volume_cbm)}::numeric[],
        ${tasks.map(t=>t.weight_kg)}::numeric[],
        ${tasks.map(t=>t.task_status)}::text[],
        ${tasks.map(t=>t.is_completed)}::boolean[],
        ${tasks.map(t=>t.is_failed)}::boolean[],
        ${tasks.map(t=>t.root_cause)}::text[],
        ${tasks.map(t=>t.operating_unit)}::text[],
        ${tasks.map(t=>t.organisation)}::text[],
        ${tasks.map(t=>t.division)}::text[],
        ${tasks.map(t=>t.internal_org)}::text[],
        ${tasks.map(t=>t.category)}::text[],
        ${tasks.map(t=>t.invoice_value)}::numeric[],
        ${tasks.map(t=>t.upload_source)}::text[]
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
        upload_source    = EXCLUDED.upload_source
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
      rows_saved:  tasks.length,
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
