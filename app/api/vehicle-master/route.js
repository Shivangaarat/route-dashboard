// app/api/vehicle-master/route.js
// POST — upload new vehicle master (vehicle entity CSV + vehicle model CSV)
// GET  — return current master summary + version history

import { neon } from '@neondatabase/serverless'

export const runtime = 'nodejs'
export const maxDuration = 30

function db() { return neon(process.env.DATABASE_URL) }

function getOwnership(fleetType) {
  if (!fleetType) return 'Unknown'
  const f = fleetType.toUpperCase().trim()
  if (f === 'CAPTIVE') return 'OWN'
  if (f === 'ADHOC' || f === 'THIRD_PARTY') return 'D-LEASED'
  return 'Unknown'
}

function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim())
  if (!lines.length) return []
  const headers = lines[0].split(',').map(h => h.replace(/^"|"$/g, '').replace(/^\uFEFF/, '').trim())
  return lines.slice(1).map(line => {
    const vals = []
    let cur = '', inQ = false
    for (const ch of line) {
      if (ch === '"') { inQ = !inQ }
      else if (ch === ',' && !inQ) { vals.push(cur.trim()); cur = '' }
      else cur += ch
    }
    vals.push(cur.trim())
    const obj = {}
    headers.forEach((h, i) => { obj[h] = (vals[i] || '').replace(/^"|"$/g, '').trim() })
    return obj
  }).filter(r => Object.values(r).some(v => v))
}

function isVirtual(vehicleId, cbm) {
  const id = (vehicleId || '').toUpperCase()
  return id.includes('SELF') || id.includes('ECOM') || parseFloat(cbm) >= 100
}

export async function POST(request) {
  try {
    const formData   = await request.formData()
    const vehicleFile = formData.get('vehicle_file')
    const modelFile   = formData.get('model_file')
    const notes       = formData.get('notes') || ''

    if (!vehicleFile || !modelFile) {
      return Response.json({ error: 'Both vehicle_file and model_file are required' }, { status: 400 })
    }

    const vehicles = parseCSV(await vehicleFile.text())
    const models   = parseCSV(await modelFile.text())

    if (!vehicles.length || !models.length) {
      return Response.json({ error: 'One or both files could not be parsed' }, { status: 400 })
    }

    const modelMap = {}
    models.forEach(m => {
      const id = m['VEHICLEMODEL ID'] || ''
      if (id) modelMap[id] = m
    })

    const merged = vehicles.map(v => {
      const vehicleId   = v['VEHICLE ID'] || ''
      const modelId     = v['VEHICLE MODEL ID'] || ''
      const model       = modelMap[modelId] || {}
      const entityCBM   = parseFloat(v['CBM']) || null
      const modelCBM    = parseFloat(model['VOLUME']) || null
      const effectiveCBM = entityCBM || modelCBM || null
      const fleetType   = model['FLEET TYPE'] || 'CAPTIVE'
      return {
        vehicle_id:         vehicleId,
        vehicle_name:       v['VEHICLE NAME'] || vehicleId,
        vehicle_model_id:   modelId,
        vehicle_model_name: model['VEHICLEMODEL NAME'] || modelId,
        fleet_type:         fleetType,
        ownership:          getOwnership(fleetType),
        team_name:          v['TEAM NAME'] || '',
        vehicle_temp:       v['VEHICLE TEMP'] || '',
        vehicle_type:       v['TYPE'] || '',
        cbm_capacity:       entityCBM,
        model_cbm_capacity: modelCBM,
        effective_cbm:      effectiveCBM,
        is_virtual:         isVirtual(vehicleId, effectiveCBM),
        is_active:          (v['ISACTIVE'] || 'Yes').toLowerCase() === 'yes',
        vehicle_reg:        v['VEHICLE REGISTRATION NO.'] || '',
      }
    }).filter(v => v.vehicle_id)

    const sql = db()

    // Archive as new version
    const ver = await sql`
      INSERT INTO vehicle_master_versions (vehicle_count, model_count, notes, vehicle_json, model_json, is_active)
      VALUES (${merged.length}, ${models.length}, ${notes}, ${JSON.stringify(merged)}::jsonb, ${JSON.stringify(models)}::jsonb, true)
      RETURNING id
    `
    const versionId = ver[0].id
    await sql`UPDATE vehicle_master_versions SET is_active = false WHERE id != ${versionId}`

    // Replace master
    await sql`TRUNCATE vehicle_master`
    for (const v of merged) {
      await sql`
        INSERT INTO vehicle_master (vehicle_id, vehicle_name, vehicle_model_id, vehicle_model_name,
          fleet_type, ownership, team_name, vehicle_temp, vehicle_type,
          cbm_capacity, model_cbm_capacity, effective_cbm, is_virtual, is_active, vehicle_reg, upload_version_id)
        VALUES (${v.vehicle_id}, ${v.vehicle_name}, ${v.vehicle_model_id}, ${v.vehicle_model_name},
          ${v.fleet_type}, ${v.ownership}, ${v.team_name}, ${v.vehicle_temp}, ${v.vehicle_type},
          ${v.cbm_capacity}, ${v.model_cbm_capacity}, ${v.effective_cbm},
          ${v.is_virtual}, ${v.is_active}, ${v.vehicle_reg}, ${versionId})
        ON CONFLICT (vehicle_id) DO UPDATE SET
          vehicle_name = EXCLUDED.vehicle_name, ownership = EXCLUDED.ownership,
          fleet_type = EXCLUDED.fleet_type, effective_cbm = EXCLUDED.effective_cbm,
          is_virtual = EXCLUDED.is_virtual, is_active = EXCLUDED.is_active,
          upload_version_id = EXCLUDED.upload_version_id
      `
    }

    return Response.json({
      status: 'success', version_id: versionId,
      total_vehicles: merged.length,
      own_vehicles:   merged.filter(v => v.ownership === 'OWN' && !v.is_virtual).length,
      dleased_vehicles: merged.filter(v => v.ownership === 'D-LEASED').length,
      virtual_excluded: merged.filter(v => v.is_virtual).length,
    })
  } catch (err) {
    console.error('[vehicle-master] POST error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}

export async function GET() {
  try {
    const sql = db()
    const [summary, versions, breakdown] = await Promise.all([
      sql`SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE ownership='OWN') AS own,
          COUNT(*) FILTER (WHERE ownership='D-LEASED') AS dleased,
          COUNT(*) FILTER (WHERE is_virtual=true) AS virtual, MAX(created_at) AS last_updated
          FROM vehicle_master`,
      sql`SELECT id, uploaded_at, vehicle_count, model_count, notes, is_active
          FROM vehicle_master_versions ORDER BY uploaded_at DESC LIMIT 10`,
      sql`SELECT ownership, fleet_type, COUNT(*) as count, ROUND(AVG(effective_cbm),2) as avg_cbm
          FROM vehicle_master WHERE is_virtual=false GROUP BY ownership, fleet_type ORDER BY ownership`
    ])
    return Response.json({ summary: summary[0], versions, breakdown })
  } catch (err) {
    return Response.json({ error: err.message }, { status: 500 })
  }
}
