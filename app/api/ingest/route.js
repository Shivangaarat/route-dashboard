// app/api/ingest/route.js
// Handles manual file uploads + triggers KPI recalculation
// Uses batch inserts for performance on large multi-day files

import { neon } from '@neondatabase/serverless'
import { recalculateDay } from '../../../lib/kpi-engine.js'
import * as XLSX from 'xlsx'

export const runtime = 'nodejs'
export const maxDuration = 60

function db() { return neon(process.env.DATABASE_URL) }

function getTempCategory(temperature) {
  if (!temperature) return 'Ambient'
  const t = temperature.toUpperCase().trim()
  return (t === 'AMBIENT' || t === '') ? 'Ambient' : 'Frozen'
}

function normaliseCity(raw) {
  if (!raw) return ''
  const c = raw.trim()
  const map = {
    'abu dhabi':'Abu Dhabi','abudhabi':'Abu Dhabi','abu_dhabi':'Abu Dhabi',
    'dubai':'Dubai','dxb':'Dubai','dxbjum':'Dubai','dxb jum':'Dubai',
    'internationalcity':'Dubai','international city':'Dubai',
    'sharjah':'Sharjah','ajman':'Ajman',
    'ras al khaimah':'Ras Al Khaimah','ras al-khaimah':'Ras Al Khaimah',
    'rasalkhaimah':'Ras Al Khaimah','rak':'Ras Al Khaimah',
    'fujairah':'Fujairah',
    'umm al quwain':'Umm Al Quwain','uaq':'Umm Al Quwain',
    'al ain':'Al Ain','alain':'Al Ain','al-ain':'Al Ain',
    'hatta':'Hatta',
  }
  return map[c.toLowerCase()] ||
    c.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ')
}

function parseDate(dateVal) {
  if (!dateVal) return null
  if (dateVal instanceof Date) return dateVal.toISOString().split('T')[0]
  const s = String(dateVal).trim()
  const ddmm = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/)
  if (ddmm) return `${ddmm[3]}-${ddmm[2].padStart(2,'0')}-${ddmm[1].padStart(2,'0')}`
  return s.split(' ')[0]
}

export async function POST(request) {
  try {
    const formData = await request.formData()
    const file     = formData.get('file')
    const source   = formData.get('source') || 'manual'

    if (!file) return Response.json({ error: 'No file provided' }, { status: 400 })

    const buffer = Buffer.from(await file.arrayBuffer())
    const wb     = XLSX.read(buffer, { type: 'buffer', cellDates: true })
    const ws     = wb.Sheets[wb.SheetNames[0]]
    const raw    = XLSX.utils.sheet_to_json(ws, { defval: '' })

    if (!raw.length) return Response.json({ error: 'No data rows found in file' }, { status: 400 })

    // Clean headers and parse all rows
    const tasks = raw.map(rawRow => {
      const row = {}
      Object.keys(rawRow).forEach(k => { row[k.replace(/^\uFEFF/,'').trim()] = rawRow[k] })

      const get = (...keys) => {
        for (const k of keys) {
          const v = row[k]
          if (v !== undefined && v !== null && v !== '') return v
        }
        return ''
      }

      const dateStr = parseDate(get('DISPATCH DATE','DELIVERY DATE','date'))
      if (!dateStr || dateStr.length < 10) return null

      const taskStatus  = String(get('TASK STATUS','TRANSACTION STATUS') || '').toUpperCase()
      const isCompleted = taskStatus === 'COMPLETED'
      const isFailed    = ['FAILED','CANCELLED','REJECTED','NOT DELIVERED','UNDELIVERED'].some(s => taskStatus.includes(s))
      const rawVol      = parseFloat(get('VOLUME','volume') || 0)
      const temperature = String(get('TEMPERATURE','temperature') || '')

      return {
        task_id:           String(get('TASK ID') || '').trim(),
        tour_id:           String(get('TOUR ID') || '').trim(),
        planned_tour_name: String(get('PLANNED TOUR NAME') || get('TOUR ID') || '').trim(),
        dispatch_date:     dateStr,
        team:              String(get('TEAM NAME','team') || '').trim(),
        temperature,
        temp_category:     getTempCategory(temperature),
        zone:              String(get('ZONE','zone') || '').trim(),
        city:              normaliseCity(String(get('CITY','city') || '')),
        rider_id:          String(get('RIDER ID','driver_id') || '').trim(),
        rider_name:        String(get('RIDER NAME','driver_name') || '').trim(),
        vehicle_id:        String(get('VEHICLE ID') || '').trim(),
        vehicle_name:      String(get('VEHICLE NAME') || '').trim(),
        vehicle_reg:       String(get('VEHICLE REGISTRATION NUMBER') || '').trim(),
        location_id:       String(get('LOCATION ID','sites') || '').trim(),
        location_name:     String(get('LOCATION NAME') || '').trim(),
        customer_name:     String(get('CUSTOMER NAME') || '').trim(),
        volume_cbm:        rawVol,
        weight_kg:         parseFloat(get('WEIGHT') || 0),
        task_status:       taskStatus,
        is_completed:      isCompleted,
        is_failed:         isFailed,
        root_cause:        String(get('ROOT CAUSE','Cancel reason') || '').trim(),
        organisation:      String(get('ORGANISATION') || '').trim(),
        division:          String(get('DIVISION') || '').trim(),
        internal_org:      String(get('INTERNAL ORG') || '').trim(),
        category:          String(get('CATEGORY') || '').trim(),
        invoice_value:     parseFloat(get('INVOICE VALUE') || 0) || null,
        upload_source:     source,
      }
    }).filter(t => t && t.task_id && t.dispatch_date)

    if (!tasks.length) return Response.json({ error: 'No valid rows after parsing. Check column headers.' }, { status: 400 })

    const sql = db()
    const dates = [...new Set(tasks.map(t => t.dispatch_date))].sort()

    // ── Batch upsert raw_tasks ────────────────────────────────────────────────
    // Process in chunks of 100 to stay within query limits
    const CHUNK = 100
    let upserted = 0

    for (let i = 0; i < tasks.length; i += CHUNK) {
      const chunk = tasks.slice(i, i + CHUNK)

      // Build values string for batch insert
      const values = chunk.map(t => ({
        task_id:           t.task_id,
        tour_id:           t.tour_id,
        planned_tour_name: t.planned_tour_name,
        dispatch_date:     t.dispatch_date,
        team:              t.team,
        temperature:       t.temperature,
        temp_category:     t.temp_category,
        zone:              t.zone,
        city:              t.city,
        rider_id:          t.rider_id,
        rider_name:        t.rider_name,
        vehicle_id:        t.vehicle_id,
        vehicle_name:      t.vehicle_name,
        vehicle_reg:       t.vehicle_reg,
        location_id:       t.location_id,
        location_name:     t.location_name,
        customer_name:     t.customer_name,
        volume_cbm:        t.volume_cbm,
        weight_kg:         t.weight_kg,
        task_status:       t.task_status,
        is_completed:      t.is_completed,
        is_failed:         t.is_failed,
        root_cause:        t.root_cause,
        organisation:      t.organisation,
        division:          t.division,
        internal_org:      t.internal_org,
        category:          t.category,
        invoice_value:     t.invoice_value,
        upload_source:     t.upload_source,
      }))

      for (const t of values) {
        await sql`
          INSERT INTO raw_tasks (
            task_id, tour_id, planned_tour_name, dispatch_date, team,
            temperature, temp_category, zone, city,
            rider_id, rider_name, vehicle_id, vehicle_name, vehicle_reg,
            location_id, location_name, customer_name,
            volume_cbm, weight_kg, task_status, is_completed, is_failed,
            root_cause, organisation, division, internal_org, category,
            invoice_value, upload_source
          ) VALUES (
            ${t.task_id}, ${t.tour_id}, ${t.planned_tour_name}, ${t.dispatch_date}, ${t.team},
            ${t.temperature}, ${t.temp_category}, ${t.zone}, ${t.city},
            ${t.rider_id}, ${t.rider_name}, ${t.vehicle_id}, ${t.vehicle_name}, ${t.vehicle_reg},
            ${t.location_id}, ${t.location_name}, ${t.customer_name},
            ${t.volume_cbm}, ${t.weight_kg}, ${t.task_status}, ${t.is_completed}, ${t.is_failed},
            ${t.root_cause}, ${t.organisation}, ${t.division}, ${t.internal_org}, ${t.category},
            ${t.invoice_value}, ${t.upload_source}
          )
          ON CONFLICT (task_id, dispatch_date) DO UPDATE SET
            task_status   = EXCLUDED.task_status,
            is_completed  = EXCLUDED.is_completed,
            is_failed     = EXCLUDED.is_failed,
            root_cause    = EXCLUDED.root_cause,
            upload_source = EXCLUDED.upload_source
        `
        upserted++
      }
    }

    // ── Batch upsert task_attempt_history ─────────────────────────────────────
    // Group by task_id to find re-deliveries efficiently
    const taskGroups = {}
    tasks.forEach(t => {
      if (!taskGroups[t.task_id]) taskGroups[t.task_id] = []
      taskGroups[t.task_id].push(t)
    })

    // Only process tasks that failed or completed (not pending)
    const significantTasks = tasks.filter(t => t.is_completed || t.is_failed)

    for (const t of significantTasks) {
      try {
        const existing = await sql`
          SELECT COUNT(*) as cnt FROM task_attempt_history WHERE task_id = ${t.task_id}
        `
        const attemptNum = parseInt(existing[0].cnt) + 1
        const firstResult = await sql`
          SELECT MIN(attempt_date) as first FROM task_attempt_history WHERE task_id = ${t.task_id}
        `
        const firstDate = firstResult[0]?.first || t.dispatch_date
        const daysSince = Math.floor((new Date(t.dispatch_date) - new Date(firstDate)) / 86400000)

        await sql`
          INSERT INTO task_attempt_history
            (task_id, attempt_number, attempt_date, status, root_cause,
             rider_id, vehicle_id, planned_tour_name, is_final_success, days_since_first)
          VALUES
            (${t.task_id}, ${attemptNum}, ${t.dispatch_date}, ${t.task_status},
             ${t.root_cause}, ${t.rider_id}, ${t.vehicle_id}, ${t.planned_tour_name},
             ${t.is_completed}, ${daysSince})
          ON CONFLICT (task_id, attempt_date) DO UPDATE SET
            status = EXCLUDED.status,
            is_final_success = EXCLUDED.is_final_success
        `
      } catch (e) {
        // Skip attempt history errors — don't fail the whole upload
      }
    }

    // ── Run KPI engine per date ───────────────────────────────────────────────
    const kpiResults = []
    for (const date of dates) {
      try {
        const result = await recalculateDay(date)
        kpiResults.push(result)
      } catch (e) {
        kpiResults.push({ date, error: e.message })
      }
    }

    return Response.json({
      status:      'success',
      rows_parsed: tasks.length,
      rows_saved:  upserted,
      dates,
      kpi_results: kpiResults,
    })

  } catch (err) {
    console.error('[ingest] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}
