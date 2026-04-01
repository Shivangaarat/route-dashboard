// direct_upload.js
// Run this locally to upload large Excel files directly to Neon
// Usage: node direct_upload.js "path/to/your/file.xlsx"

const ExcelJS = require('exceljs')
const { neon } = require('@neondatabase/serverless')

// ── PASTE YOUR DATABASE_URL HERE ──────────────────────────────────────────────
const DATABASE_URL = 'postgresql://neondb_owner:npg_O8mYbA4aSlWh@ep-little-bonus-amkepsa4-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
// ─────────────────────────────────────────────────────────────────────────────

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

async function insertBatch(sql, batch) {
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
      ${batch.map(t=>t.task_id)}::text[],
      ${batch.map(t=>t.base_task_id)}::text[],
      ${batch.map(t=>t.tour_id)}::text[],
      ${batch.map(t=>t.planned_tour_name)}::text[],
      ${batch.map(t=>t.dispatch_date)}::date[],
      ${batch.map(t=>t.team)}::text[],
      ${batch.map(t=>t.temperature)}::text[],
      ${batch.map(t=>t.temp_category)}::text[],
      ${batch.map(t=>t.zone)}::text[],
      ${batch.map(t=>t.city)}::text[],
      ${batch.map(t=>t.rider_id)}::text[],
      ${batch.map(t=>t.rider_name)}::text[],
      ${batch.map(t=>t.vehicle_id)}::text[],
      ${batch.map(t=>t.vehicle_name)}::text[],
      ${batch.map(t=>t.vehicle_reg)}::text[],
      ${batch.map(t=>t.location_id)}::text[],
      ${batch.map(t=>t.location_name)}::text[],
      ${batch.map(t=>t.customer_name)}::text[],
      ${batch.map(t=>t.volume_cbm)}::numeric[],
      ${batch.map(t=>t.weight_kg)}::numeric[],
      ${batch.map(t=>t.task_status)}::text[],
      ${batch.map(t=>t.is_completed)}::boolean[],
      ${batch.map(t=>t.is_failed)}::boolean[],
      ${batch.map(t=>t.root_cause)}::text[],
      ${batch.map(t=>t.operating_unit)}::text[],
      ${batch.map(t=>t.organisation)}::text[],
      ${batch.map(t=>t.division)}::text[],
      ${batch.map(t=>t.internal_org)}::text[],
      ${batch.map(t=>t.category)}::text[],
      ${batch.map(t=>t.invoice_value)}::numeric[],
      ${batch.map(()=>'direct_upload')}::text[]
    ) AS t(task_id,base_task_id,tour_id,planned_tour_name,dispatch_date,team,
           temperature,temp_category,zone,city,
           rider_id,rider_name,vehicle_id,vehicle_name,vehicle_reg,
           location_id,location_name,customer_name,
           volume_cbm,weight_kg,task_status,is_completed,is_failed,
           root_cause,operating_unit,organisation,division,internal_org,category,
           invoice_value,upload_source)
    ON CONFLICT (task_id, dispatch_date) DO UPDATE SET
      task_status    = EXCLUDED.task_status,
      is_completed   = EXCLUDED.is_completed,
      is_failed      = EXCLUDED.is_failed,
      root_cause     = EXCLUDED.root_cause,
      operating_unit = EXCLUDED.operating_unit,
      upload_source  = EXCLUDED.upload_source
  `
}

async function main() {
  const filePath = process.argv[2]
  if (!filePath) {
    console.error('Usage: node direct_upload.js "path/to/file.xlsx"')
    process.exit(1)
  }

  console.log(`Reading file: ${filePath}`)
  const workbook = new ExcelJS.Workbook()
  await workbook.xlsx.readFile(filePath)

  const ws = workbook.worksheets[0]
  console.log(`Sheet: ${ws.name}, rows: ${ws.rowCount}`)

  const headerRow = ws.getRow(1)
  const headers = []
  headerRow.eachCell({ includeEmpty: true }, (cell) => {
    headers.push(String(cell.value || '').replace(/^\uFEFF/, '').trim().toUpperCase())
  })
  console.log(`Headers found: ${headers.length}`)

  const sql = neon(DATABASE_URL)
  const BATCH = 500
  let batch = []
  let totalInserted = 0

  for (let i = 2; i <= ws.rowCount; i++) {
    const row = ws.getRow(i)
    const g = (...keys) => {
      for (const k of keys) {
        const idx = headers.indexOf(k)
        if (idx !== -1) {
          const cell = row.getCell(idx + 1)
          let v = cell.value
          if (v instanceof Date) return v
          if (v !== null && v !== undefined && v !== '') return String(v).trim()
        }
      }
      return ''
    }

    const dateStr = parseDate(g('DISPATCH DATE', 'DELIVERY DATE'))
    if (!dateStr || dateStr.length < 10) continue
    const taskId = String(g('TASK ID') || '').trim()
    if (!taskId) continue

    const taskStatus = String(g('TASK STATUS', 'TRANSACTION STATUS') || '').toUpperCase()
    const temperature = String(g('TEMPERATURE') || '')
    const baseTaskId = taskId.replace(/-\d{4}-\d{2}-\d{2}-\d+$/, '')

    batch.push({
      task_id:           taskId,
      base_task_id:      baseTaskId,
      tour_id:           String(g('TOUR ID') || ''),
      planned_tour_name: String(g('PLANNED TOUR NAME') || g('TOUR ID') || ''),
      dispatch_date:     dateStr,
      team:              String(g('TEAM NAME') || ''),
      temperature,
      temp_category:     getTempCategory(temperature),
      zone:              String(g('ZONE') || ''),
      city:              normaliseCity(String(g('CITY') || '')),
      rider_id:          String(g('RIDER ID') || ''),
      rider_name:        String(g('RIDER NAME') || ''),
      vehicle_id:        String(g('VEHICLE ID') || ''),
      vehicle_name:      String(g('VEHICLE NAME') || ''),
      vehicle_reg:       String(g('VEHICLE REGISTRATION NUMBER') || ''),
      location_id:       String(g('LOCATION ID') || ''),
      location_name:     String(g('LOCATION NAME') || ''),
      customer_name:     String(g('CUSTOMER NAME') || ''),
      volume_cbm:        parseFloat(g('VOLUME') || 0) || 0,
      weight_kg:         parseFloat(g('WEIGHT') || 0) || 0,
      task_status:       taskStatus,
      is_completed:      taskStatus === 'COMPLETED',
      is_failed:         ['FAILED','CANCELLED','REJECTED','NOT DELIVERED','UNDELIVERED'].some(s=>taskStatus.includes(s)),
      root_cause:        String(g('ROOT CAUSE', 'Cancel reason') || ''),
      operating_unit:    String(g('OPERATING UNIT', 'operating_unit') || ''),
      organisation:      String(g('ORGANISATION') || ''),
      division:          String(g('DIVISION') || ''),
      internal_org:      String(g('INTERNAL ORG') || ''),
      category:          String(g('CATEGORY') || ''),
      invoice_value:     parseFloat(g('INVOICE VALUE') || 0) || null,
    })

    if (batch.length >= BATCH) {
      await insertBatch(sql, batch)
      totalInserted += batch.length
      console.log(`Inserted ${totalInserted} rows...`)
      batch = []
    }
  }

  if (batch.length > 0) {
    await insertBatch(sql, batch)
    totalInserted += batch.length
  }

  console.log(`\n✓ Done! ${totalInserted} rows uploaded to Neon.`)
}

main().catch(err => { console.error('Error:', err.message); process.exit(1) })
