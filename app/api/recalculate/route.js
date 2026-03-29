// app/api/recalculate/route.js
// POST { date: '2026-03-19' } — runs KPI engine for one date

import { recalculateDay } from '../../../lib/kpi-engine.js'

export const runtime = 'nodejs'
export const maxDuration = 60

export async function POST(request) {
  try {
    const { date } = await request.json()
    if (!date) return Response.json({ error: 'date required' }, { status: 400 })
    const result = await recalculateDay(date)
    return Response.json({ status: 'success', result })
  } catch (err) {
    console.error('[recalculate] Error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}
