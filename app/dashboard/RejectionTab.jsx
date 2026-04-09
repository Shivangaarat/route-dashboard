'use client'
// app/components/RejectionTab.jsx
// Drop-in tab component for the Route Pattern Intelligence Dashboard
//
// Props: none (self-contained, fetches from /api/rejections)
//
// Usage in your dashboard page:
//   import RejectionTab from '@/components/RejectionTab'
//   ...
//   { id: 'rejections', label: '🚫 Rejections', component: <RejectionTab /> }

import { useState, useEffect, useCallback } from 'react'

// ─────────────────────────────────────────────────────────────────────────────
// Colour palette — one per reason card (cycles if > 3)
// ─────────────────────────────────────────────────────────────────────────────
const PALETTE = [
  { bg: '#FEF3C7', border: '#F59E0B', bar: '#F59E0B', text: '#92400E', badge: '#FDE68A' },
  { bg: '#FEE2E2', border: '#EF4444', bar: '#EF4444', text: '#991B1B', badge: '#FECACA' },
  { bg: '#EDE9FE', border: '#8B5CF6', bar: '#8B5CF6', text: '#5B21B6', badge: '#DDD6FE' },
]

// ─────────────────────────────────────────────────────────────────────────────
// Tiny inline bar (no external lib needed)
// ─────────────────────────────────────────────────────────────────────────────
function MiniBar({ value, max, color }) {
  const pct = max > 0 ? Math.round((value / max) * 100) : 0
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, width: '100%' }}>
      <div style={{
        flex: 1, height: 8, background: '#E5E7EB', borderRadius: 4, overflow: 'hidden',
      }}>
        <div style={{
          width: `${pct}%`, height: '100%', background: color,
          borderRadius: 4, transition: 'width 0.4s ease',
        }} />
      </div>
      <span style={{ minWidth: 28, fontSize: 12, color: '#6B7280', textAlign: 'right' }}>
        {value}
      </span>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Sparkline (SVG, no lib)
// ─────────────────────────────────────────────────────────────────────────────
function Sparkline({ data, color, width = 120, height = 36 }) {
  if (!data || data.length < 2) return null
  const vals = data.map(d => d.count)
  const max  = Math.max(...vals, 1)
  const step = width / (vals.length - 1)
  const pts  = vals.map((v, i) => `${i * step},${height - (v / max) * (height - 4)}`).join(' ')
  return (
    <svg width={width} height={height} style={{ overflow: 'visible' }}>
      <polyline
        points={pts}
        fill="none"
        stroke={color}
        strokeWidth={2}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      {vals.map((v, i) => (
        <circle key={i} cx={i * step} cy={height - (v / max) * (height - 4)}
          r={3} fill={color} />
      ))}
    </svg>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Reason Card
// ─────────────────────────────────────────────────────────────────────────────
function ReasonCard({ data, rank, palette, isSelected, onSelect }) {
  const maxClient = data.top_clients[0]?.failed_count || 1
  return (
    <div
      onClick={() => onSelect(data.reason)}
      style={{
        background:    isSelected ? palette.bg : '#fff',
        border:        `2px solid ${isSelected ? palette.border : '#E5E7EB'}`,
        borderRadius:  12,
        padding:       '18px 20px',
        cursor:        'pointer',
        transition:    'all 0.2s',
        boxShadow:     isSelected ? `0 4px 14px ${palette.border}33` : '0 1px 4px #0001',
        flex:          '1 1 0',
        minWidth:      240,
      }}
    >
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 10 }}>
        <div>
          <span style={{
            display: 'inline-block', background: palette.badge,
            color: palette.text, borderRadius: 6, fontSize: 11,
            fontWeight: 700, padding: '2px 8px', marginBottom: 6,
          }}>
            #{rank} Reason
          </span>
          <div style={{
            fontSize: 15, fontWeight: 700, color: '#1F2937',
            lineHeight: 1.3, maxWidth: 200,
          }}>
            {data.reason}
          </div>
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={{ fontSize: 26, fontWeight: 800, color: palette.border, lineHeight: 1 }}>
            {data.total_failed}
          </div>
          <div style={{ fontSize: 11, color: '#9CA3AF' }}>failures</div>
        </div>
      </div>

      {/* Stats row */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 18, fontWeight: 700, color: '#374151' }}>{data.pct_of_all_failures}%</div>
          <div style={{ fontSize: 10, color: '#9CA3AF' }}>of all failures</div>
        </div>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 18, fontWeight: 700, color: '#374151' }}>{data.unique_clients}</div>
          <div style={{ fontSize: 10, color: '#9CA3AF' }}>clients affected</div>
        </div>
        {data.trend.length > 1 && (
          <div style={{ marginLeft: 'auto', opacity: 0.8 }}>
            <Sparkline data={data.trend} color={palette.bar} />
          </div>
        )}
      </div>

      {/* Top 3 clients preview */}
      <div style={{ borderTop: `1px solid ${palette.badge}`, paddingTop: 10 }}>
        <div style={{ fontSize: 11, color: '#6B7280', marginBottom: 6, fontWeight: 600 }}>
          TOP CLIENTS
        </div>
        {data.top_clients.slice(0, 3).map((c, i) => (
          <div key={i} style={{ marginBottom: 5 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
              <span style={{
                fontSize: 12, color: '#374151', maxWidth: 180,
                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              }}>
                {c.client}
              </span>
              <span style={{ fontSize: 12, color: '#6B7280' }}>{c.city || '—'}</span>
            </div>
            <MiniBar value={c.failed_count} max={maxClient} color={palette.bar} />
          </div>
        ))}
      </div>

      <div style={{
        marginTop: 10, fontSize: 11, color: palette.text, textAlign: 'center',
        fontWeight: 500, opacity: isSelected ? 1 : 0.6,
      }}>
        {isSelected ? '▲ Click again to collapse' : '▼ Click to drill down'}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Drill-down Table
// ─────────────────────────────────────────────────────────────────────────────
function DrilldownTable({ reason, clients, palette }) {
  const [sortKey, setSortKey] = useState('failed_count')
  const [sortDir, setSortDir] = useState('desc')
  const [search, setSearch] = useState('')

  const toggleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
  }

  const filtered = clients
    .filter(c =>
      !search ||
      c.client.toLowerCase().includes(search.toLowerCase()) ||
      (c.city || '').toLowerCase().includes(search.toLowerCase()) ||
      (c.zone || '').toLowerCase().includes(search.toLowerCase())
    )
    .sort((a, b) => {
      const av = a[sortKey] ?? 0
      const bv = b[sortKey] ?? 0
      if (typeof av === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? av - bv : bv - av
    })

  const SortIcon = ({ k }) => {
    if (sortKey !== k) return <span style={{ opacity: 0.3 }}> ↕</span>
    return <span> {sortDir === 'asc' ? '↑' : '↓'}</span>
  }

  const thStyle = (k) => ({
    padding: '10px 14px', textAlign: 'left', fontSize: 12, fontWeight: 700,
    color: '#374151', background: palette.bg, cursor: 'pointer',
    borderBottom: `2px solid ${palette.border}`, whiteSpace: 'nowrap',
  })
  const tdStyle = { padding: '9px 14px', fontSize: 13, color: '#374151', borderBottom: '1px solid #F3F4F6' }

  return (
    <div style={{
      marginTop: 16, background: '#fff', borderRadius: 12,
      border: `2px solid ${palette.border}`, overflow: 'hidden',
      boxShadow: `0 4px 20px ${palette.border}22`,
    }}>
      {/* Drilldown header */}
      <div style={{
        background: palette.bg, padding: '14px 20px',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 10,
      }}>
        <div>
          <span style={{ fontSize: 14, fontWeight: 700, color: palette.text }}>
            Clients — "{reason}"
          </span>
          <span style={{ fontSize: 12, color: '#6B7280', marginLeft: 10 }}>
            {clients.length} unique client{clients.length !== 1 ? 's' : ''}
          </span>
        </div>
        <input
          type="text"
          placeholder="Search client / city / zone…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{
            padding: '6px 12px', fontSize: 13, border: `1px solid ${palette.border}`,
            borderRadius: 8, outline: 'none', background: '#fff', minWidth: 220,
          }}
        />
      </div>

      {/* Table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={{ ...thStyle(''), width: 36, textAlign: 'center' }}>#</th>
              <th style={thStyle('client')} onClick={() => toggleSort('client')}>
                Client <SortIcon k="client" />
              </th>
              <th style={thStyle('city')} onClick={() => toggleSort('city')}>
                City <SortIcon k="city" />
              </th>
              <th style={thStyle('zone')} onClick={() => toggleSort('zone')}>
                Zone <SortIcon k="zone" />
              </th>
              <th style={thStyle('failed_count')} onClick={() => toggleSort('failed_count')}>
                Failed Orders <SortIcon k="failed_count" />
              </th>
              <th style={thStyle('invoice_value_lost')} onClick={() => toggleSort('invoice_value_lost')}>
                Invoice Value Lost <SortIcon k="invoice_value_lost" />
              </th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((c, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? '#fff' : '#FAFAFA' }}>
                <td style={{ ...tdStyle, textAlign: 'center', color: '#9CA3AF', fontSize: 12 }}>{i + 1}</td>
                <td style={{ ...tdStyle, fontWeight: 600 }}>{c.client}</td>
                <td style={tdStyle}>{c.city || '—'}</td>
                <td style={tdStyle}>{c.zone || '—'}</td>
                <td style={{ ...tdStyle, textAlign: 'center' }}>
                  <span style={{
                    display: 'inline-block', background: palette.badge,
                    color: palette.text, borderRadius: 6, padding: '2px 10px',
                    fontWeight: 700, fontSize: 13,
                  }}>
                    {c.failed_count}
                  </span>
                </td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  {c.invoice_value_lost != null
                    ? `AED ${Number(c.invoice_value_lost).toLocaleString('en-AE', { minimumFractionDigits: 2 })}`
                    : '—'}
                </td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={6} style={{ padding: '24px', textAlign: 'center', color: '#9CA3AF', fontSize: 13 }}>
                  No clients match your search.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Main RejectionTab component
// ─────────────────────────────────────────────────────────────────────────────
export default function RejectionTab() {
  // Date helpers
  const today    = new Date().toISOString().split('T')[0]
  const weekAgo  = new Date(Date.now() - 6 * 864e5).toISOString().split('T')[0]

  const [dateFrom, setDateFrom]       = useState(weekAgo)
  const [dateTo, setDateTo]           = useState(today)
  const [dateMode, setDateMode]       = useState('range') // 'single' | 'range'
  const [data, setData]               = useState(null)
  const [loading, setLoading]         = useState(false)
  const [error, setError]             = useState(null)
  const [selectedReason, setSelected] = useState(null)

  const effectiveDateTo = dateMode === 'single' ? dateFrom : dateTo

  const fetchData = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const params = new URLSearchParams({
        date_from: dateFrom,
        date_to:   effectiveDateTo,
        top_n:     3,
      })
      const res  = await fetch(`/api/rejections?${params}`)
      const json = await res.json()
      if (!res.ok) throw new Error(json.error || 'Server error')
      setData(json)
      // auto-select top reason
      if (json.reasons?.length) setSelected(json.reasons[0].reason)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [dateFrom, effectiveDateTo])

  useEffect(() => { fetchData() }, [fetchData])

  const handleReasonClick = (reason) => {
    setSelected(prev => prev === reason ? null : reason)
  }

  const selectedData = data?.reasons.find(r => r.reason === selectedReason)
  const selectedPalette = data
    ? PALETTE[data.reasons.findIndex(r => r.reason === selectedReason) % PALETTE.length]
    : PALETTE[0]

  // ── Quick date presets ──────────────────────────────────────────────────────
  const applyPreset = (preset) => {
    const now = new Date()
    const fmt = d => d.toISOString().split('T')[0]
    setDateMode('range')
    if (preset === 'today') {
      setDateMode('single'); setDateFrom(fmt(now))
    } else if (preset === '7d') {
      setDateFrom(fmt(new Date(now - 6 * 864e5))); setDateTo(fmt(now))
    } else if (preset === '30d') {
      setDateFrom(fmt(new Date(now - 29 * 864e5))); setDateTo(fmt(now))
    } else if (preset === 'mtd') {
      setDateFrom(fmt(new Date(now.getFullYear(), now.getMonth(), 1))); setDateTo(fmt(now))
    }
  }

  return (
    <div style={{ fontFamily: 'Inter, system-ui, sans-serif', padding: '0 0 40px' }}>

      {/* ── Date Controls ─────────────────────────────────────────────────────── */}
      <div style={{
        background: '#F9FAFB', border: '1px solid #E5E7EB',
        borderRadius: 12, padding: '14px 20px', marginBottom: 24,
        display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 12,
      }}>
        {/* Mode toggle */}
        <div style={{ display: 'flex', gap: 0, borderRadius: 8, overflow: 'hidden', border: '1px solid #D1D5DB' }}>
          {['single', 'range'].map(m => (
            <button key={m} onClick={() => setDateMode(m)} style={{
              padding: '6px 14px', fontSize: 12, fontWeight: 600, cursor: 'pointer',
              background: dateMode === m ? '#1F2937' : '#fff',
              color: dateMode === m ? '#fff' : '#6B7280',
              border: 'none',
            }}>
              {m === 'single' ? 'Single Day' : 'Date Range'}
            </button>
          ))}
        </div>

        {/* Date inputs */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <label style={{ fontSize: 12, color: '#6B7280', fontWeight: 500 }}>
            {dateMode === 'single' ? 'Date' : 'From'}
          </label>
          <input type="date" value={dateFrom} max={today}
            onChange={e => setDateFrom(e.target.value)}
            style={{ padding: '6px 10px', borderRadius: 8, border: '1px solid #D1D5DB', fontSize: 13 }}
          />
          {dateMode === 'range' && (
            <>
              <span style={{ color: '#9CA3AF' }}>→</span>
              <input type="date" value={dateTo} min={dateFrom} max={today}
                onChange={e => setDateTo(e.target.value)}
                style={{ padding: '6px 10px', borderRadius: 8, border: '1px solid #D1D5DB', fontSize: 13 }}
              />
            </>
          )}
        </div>

        {/* Presets */}
        <div style={{ display: 'flex', gap: 6 }}>
          {[
            { key: 'today', label: 'Today' },
            { key: '7d',    label: '7 Days' },
            { key: '30d',   label: '30 Days' },
            { key: 'mtd',   label: 'MTD' },
          ].map(p => (
            <button key={p.key} onClick={() => applyPreset(p.key)} style={{
              padding: '5px 12px', fontSize: 12, borderRadius: 6, cursor: 'pointer',
              background: '#fff', border: '1px solid #D1D5DB', color: '#374151',
              fontWeight: 500, transition: 'all 0.15s',
            }}>
              {p.label}
            </button>
          ))}
        </div>

        <button onClick={fetchData} disabled={loading} style={{
          marginLeft: 'auto', padding: '7px 18px', borderRadius: 8, cursor: loading ? 'not-allowed' : 'pointer',
          background: '#1F2937', color: '#fff', border: 'none', fontSize: 13, fontWeight: 600,
        }}>
          {loading ? '⟳ Loading…' : '↻ Refresh'}
        </button>
      </div>

      {/* ── Error ──────────────────────────────────────────────────────────────── */}
      {error && (
        <div style={{
          background: '#FEF2F2', border: '1px solid #FECACA', color: '#991B1B',
          borderRadius: 10, padding: '12px 16px', marginBottom: 20, fontSize: 13,
        }}>
          ⚠️ {error}
        </div>
      )}

      {/* ── Loading skeleton ───────────────────────────────────────────────────── */}
      {loading && !data && (
        <div style={{ display: 'flex', gap: 16 }}>
          {[0, 1, 2].map(i => (
            <div key={i} style={{
              flex: 1, minWidth: 240, height: 220, borderRadius: 12,
              background: '#F3F4F6', animation: 'pulse 1.5s infinite',
            }} />
          ))}
        </div>
      )}

      {/* ── Empty state ────────────────────────────────────────────────────────── */}
      {!loading && data && data.total_failed === 0 && (
        <div style={{
          textAlign: 'center', padding: '60px 20px', color: '#9CA3AF',
        }}>
          <div style={{ fontSize: 48, marginBottom: 10 }}>✅</div>
          <div style={{ fontSize: 16, fontWeight: 600, color: '#6B7280' }}>No failed tasks in this period</div>
          <div style={{ fontSize: 13, marginTop: 4 }}>Try a different date range</div>
        </div>
      )}

      {/* ── Summary badge ──────────────────────────────────────────────────────── */}
      {data && data.total_failed > 0 && (
        <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{
            background: '#FEF3C7', border: '1px solid #F59E0B',
            color: '#92400E', borderRadius: 8, padding: '4px 14px',
            fontSize: 13, fontWeight: 600,
          }}>
            {data.total_failed.toLocaleString()} total failures
          </span>
          <span style={{ fontSize: 13, color: '#6B7280' }}>
            {dateMode === 'single'
              ? `on ${dateFrom}`
              : `${dateFrom} → ${effectiveDateTo}`}
          </span>
        </div>
      )}

      {/* ── Reason Cards ───────────────────────────────────────────────────────── */}
      {data && data.reasons.length > 0 && (
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 8 }}>
          {data.reasons.map((r, i) => (
            <ReasonCard
              key={r.reason}
              data={r}
              rank={i + 1}
              palette={PALETTE[i % PALETTE.length]}
              isSelected={selectedReason === r.reason}
              onSelect={handleReasonClick}
            />
          ))}
        </div>
      )}

      {/* ── Drilldown Table ────────────────────────────────────────────────────── */}
      {selectedData && (
        <DrilldownTable
          reason={selectedData.reason}
          clients={selectedData.top_clients}
          palette={selectedPalette}
        />
      )}
    </div>
  )
}
