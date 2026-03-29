'use client'
import { useState, useEffect, useCallback, useRef } from 'react'
import { Bar, Line, Doughnut } from 'react-chartjs-2'
import {
  Chart as ChartJS, CategoryScale, LinearScale, BarElement, LineElement,
  PointElement, ArcElement, Tooltip, Legend
} from 'chart.js'
ChartJS.register(CategoryScale, LinearScale, BarElement, LineElement, PointElement, ArcElement, Tooltip, Legend)

// ── Colour helpers ────────────────────────────────────────────────────────────
const C = {
  good:   '#1D9E75', warn: '#BA7517', bad: '#E24B4A',
  blue:   '#378ADD', purple: '#534AB7', gray: '#888780',
  bgGood: '#E1F5EE', bgWarn: '#FAEEDA', bgBad: '#FCEBEB', bgBlue: '#E6F1FB',
}
const pct  = v => v == null ? '—' : `${v}%`
const num  = v => v == null ? '—' : Number(v).toLocaleString()
const dec  = (v,d=2) => v == null ? '—' : Number(v).toFixed(d)
const clr  = (v, good=80, warn=60) => v == null ? C.gray : v>=good ? C.good : v>=warn ? C.warn : C.bad
const rclr = (v, good=5, warn=10) => v == null ? C.gray : v<=good ? C.good : v<=warn ? C.warn : C.bad

// ── Reusable components ───────────────────────────────────────────────────────
const Card = ({children, style={}}) => (
  <div style={{background:'#fff',border:'0.5px solid rgba(0,0,0,0.1)',borderRadius:12,padding:'1rem 1.25rem',marginBottom:'1rem',...style}}>
    {children}
  </div>
)
const SectionTitle = ({children}) => (
  <div style={{fontSize:11,fontWeight:600,color:'#73726c',textTransform:'uppercase',letterSpacing:'.05em',marginBottom:12}}>
    {children}
  </div>
)
const KPI = ({label,value,sub,color,bg}) => (
  <div style={{background:bg||'#f5f4f0',borderRadius:8,padding:'12px 14px'}}>
    <div style={{fontSize:11,color:'#73726c',marginBottom:4}}>{label}</div>
    <div style={{fontSize:22,fontWeight:600,color:color||'inherit'}}>{value}</div>
    {sub && <div style={{fontSize:11,color:'#73726c',marginTop:2}}>{sub}</div>}
  </div>
)
const Pill = ({text,color='#73726c',bg='#f5f4f0'}) => (
  <span style={{background:bg,color,fontSize:11,fontWeight:500,padding:'2px 10px',borderRadius:99,display:'inline-block'}}>{text}</span>
)
const Tab = ({label,active,onClick}) => (
  <button onClick={onClick} style={{
    padding:'7px 16px',borderRadius:8,fontSize:13,cursor:'pointer',
    border:`0.5px solid ${active?C.blue:'rgba(0,0,0,0.12)'}`,
    background:active?C.bgBlue:'transparent',
    color:active?C.blue:'#73726c',transition:'all .15s'
  }}>{label}</button>
)
const Tbl = ({cols,rows,colWidths=[]}) => (
  <div style={{overflowX:'auto'}}>
    <table style={{width:'100%',borderCollapse:'collapse',fontSize:12,tableLayout:'fixed'}}>
      <thead><tr>{cols.map((c,i)=>(
        <th key={i} style={{textAlign:'left',padding:'8px',color:'#73726c',borderBottom:'0.5px solid rgba(0,0,0,0.1)',fontWeight:500,fontSize:11,width:colWidths[i]||'auto',overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{c}</th>
      ))}</tr></thead>
      <tbody>{rows.map((r,i)=>(
        <tr key={i} style={{background:i%2===0?'transparent':'#fafaf8'}}>
          {r.map((cell,j)=>(
            <td key={j} style={{padding:'8px',borderBottom:'0.5px solid rgba(0,0,0,0.06)',overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{cell}</td>
          ))}
        </tr>
      ))}</tbody>
    </table>
  </div>
)
const Bar6 = ({v,max=100,color}) => (
  <div style={{display:'flex',alignItems:'center',gap:6}}>
    <div style={{flex:1,height:6,background:'rgba(0,0,0,0.08)',borderRadius:3,overflow:'hidden'}}>
      <div style={{height:'100%',width:`${Math.min(100,v||0)}%`,background:color||C.blue,borderRadius:3}}/>
    </div>
    <span style={{fontSize:11,color:'#73726c',minWidth:36,textAlign:'right'}}>{pct(v)}</span>
  </div>
)

// ── Metric matrix row ─────────────────────────────────────────────────────────
const MetricRow = ({label, overall, ambient, frozen, hc, format='num', goodHigh=true}) => {
  const fmt = v => {
    if (v == null || v === undefined) return <span style={{color:'#ccc'}}>—</span>
    if (format==='pct') return <span style={{color:goodHigh?clr(v):rclr(v)}}>{pct(v)}</span>
    if (format==='dec') return dec(v)
    return num(v)
  }
  return (
    <tr>
      <td style={{padding:'8px 10px',fontSize:12,borderBottom:'0.5px solid rgba(0,0,0,0.06)',fontWeight:400}}>{label}</td>
      <td style={{padding:'8px 10px',fontSize:12,borderBottom:'0.5px solid rgba(0,0,0,0.06)',textAlign:'center',fontWeight:500}}>{fmt(overall)}</td>
      <td style={{padding:'8px 10px',fontSize:12,borderBottom:'0.5px solid rgba(0,0,0,0.06)',textAlign:'center'}}>{fmt(ambient)}</td>
      <td style={{padding:'8px 10px',fontSize:12,borderBottom:'0.5px solid rgba(0,0,0,0.06)',textAlign:'center'}}>{fmt(frozen)}</td>
      <td style={{padding:'8px 10px',fontSize:12,borderBottom:'0.5px solid rgba(0,0,0,0.06)',textAlign:'center'}}>{fmt(hc)}</td>
    </tr>
  )
}

const MetricMatrix = ({data, title}) => {
  const get = (cat, key) => data?.find(r=>r.analysis_category===cat)?.[key]
  const O = cat => ({
    overall_avg_drops:      get('Overall',cat==='overall_avg_drops'?'overall_avg_drops':cat),
  })
  const bycat = (key) => ({
    overall: get('Overall', key),
    ambient: get('NHC Ambient', key),
    frozen:  get('NHC Frozen', key),
    hc:      get('HC', key),
  })
  return (
    <Card>
      {title && <SectionTitle>{title}</SectionTitle>}
      <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
        <thead>
          <tr style={{background:'#1F3864'}}>
            <th style={{padding:'9px 10px',color:'#fff',fontWeight:500,fontSize:11,textAlign:'left',width:240}}>Metric</th>
            <th style={{padding:'9px 10px',color:'#fff',fontWeight:500,fontSize:11,textAlign:'center'}}>Overall</th>
            <th style={{padding:'9px 10px',color:'#fff',fontWeight:500,fontSize:11,textAlign:'center'}}>NHC Ambient</th>
            <th style={{padding:'9px 10px',color:'#fff',fontWeight:500,fontSize:11,textAlign:'center'}}>NHC Frozen</th>
            <th style={{padding:'9px 10px',color:'#fff',fontWeight:500,fontSize:11,textAlign:'center'}}>HC</th>
          </tr>
        </thead>
        <tbody>
          <MetricRow label="Overall avg drops"             {...bycat('overall_avg_drops')}    format="dec"/>
          <MetricRow label="Avg drops excl. single drop"  {...bycat('avg_drops_excl_single')} format="dec"/>
          <MetricRow label="Count of single drops"        {...bycat('single_drop_count')}     format="num"/>
          <MetricRow label="OWN vehicles"                 {...bycat('own_vehicles')}          format="num"/>
          <MetricRow label="OWN drops"                    {...bycat('own_drops')}             format="num"/>
          <MetricRow label="OWN avg drops/vehicle"        {...bycat('own_avg_drops')}         format="dec"/>
          <MetricRow label="D-LEASED vehicles"            {...bycat('dleased_vehicles')}      format="num"/>
          <MetricRow label="D-LEASED drops"               {...bycat('dleased_drops')}         format="num"/>
          <MetricRow label="D-LEASED avg drops/vehicle"   {...bycat('dleased_avg_drops')}     format="dec"/>
          <MetricRow label="Multi-drop vehicles"          {...bycat('multi_drop_vehicle_count')} format="num"/>
          <MetricRow label="Multi-drop total"             {...bycat('multi_drop_total')}      format="num"/>
          <MetricRow label="Single-drop vehicles"         {...bycat('single_drop_vehicle_count')} format="num"/>
          <MetricRow label="Single-drop total"            {...bycat('single_drop_total')}     format="num"/>
          <MetricRow label="Bulk routes"                  {...bycat('bulk_route_count')}      format="num"/>
          <MetricRow label="Truck util % — CBM"           {...bycat('avg_volume_util_pct')}   format="pct" goodHigh={true}/>
          <MetricRow label="Truck util % — Pallet"        {...bycat('avg_pallet_util_pct')}   format="pct" goodHigh={true}/>
          <MetricRow label="Rejection %"                  {...bycat('daily_rejection_pct')}   format="pct" goodHigh={false}/>
          <MetricRow label="RD %"                         {...bycat('rd_pct')}                format="pct" goodHigh={false}/>
          <MetricRow label="RD Pharma %"                  {...bycat('rd_pharma_pct')}         format="pct" goodHigh={false}/>
          <MetricRow label="RD Medlab %"                  {...bycat('rd_medlab_pct')}         format="pct" goodHigh={false}/>
          <MetricRow label="First attempt success %"      {...bycat('first_attempt_success_pct')} format="pct" goodHigh={true}/>
        </tbody>
      </table>
    </Card>
  )
}

// ── Tour table ────────────────────────────────────────────────────────────────
const TourTable = ({tours, filter}) => {
  const filtered = tours.filter(t =>
    (filter==='all' || t.analysis_category===filter) && !t.is_excluded
  )
  if (!filtered.length) return <div style={{color:'#73726c',fontSize:13,padding:'1rem 0'}}>No tours found for this filter.</div>
  return (
    <Tbl
      cols={['Tour','Team','Temp','Zone','Vehicle','Own','Orders','Drops','Vol CBM','Util%','Type','Bulk','Rej%']}
      colWidths={[140,60,70,80,80,70,65,55,75,65,65,50,55]}
      rows={filtered.map(t=>[
        <span style={{fontWeight:500}}>{t.planned_tour_name}</span>,
        t.team,
        <Pill text={t.temp_category} color={t.temp_category==='Frozen'?C.blue:C.good} bg={t.temp_category==='Frozen'?C.bgBlue:C.bgGood}/>,
        t.zone||'—',
        t.vehicle_id||'—',
        <Pill text={t.ownership||'—'} color={t.ownership==='OWN'?C.good:C.warn} bg={t.ownership==='OWN'?C.bgGood:C.bgWarn}/>,
        num(t.total_orders),
        num(t.unique_drops),
        dec(t.total_volume_cbm,3),
        t.volume_util_pct!=null ? <span style={{color:clr(t.volume_util_pct)}}>{pct(t.volume_util_pct)}</span> : '—',
        t.route_type,
        t.is_bulk ? <Pill text="BULK" color={C.purple} bg="#EEEDFE"/> : '—',
        <span style={{color:rclr(t.rejection_pct)}}>{pct(t.rejection_pct)}</span>,
      ])}
    />
  )
}

// ── Emirates table ────────────────────────────────────────────────────────────
const EmiratesTable = ({data, category}) => {
  // For Overall: aggregate all categories by city
  // For specific category: filter to that category only
  let rows = []
  if (category === 'Overall') {
    const cityMap = {}
    data.forEach(r => {
      if (!r.city) return
      if (!cityMap[r.city]) cityMap[r.city] = {city:r.city,total_orders:0,total_drops:0,completed_orders:0,failed_orders:0,total_volume_cbm:0}
      // Only count each city once per category to avoid double counting
    })
    // Use the Overall rows directly from the API (already aggregated)
    rows = data.filter(r => r.analysis_category === 'Overall')
    if (!rows.length) {
      // Fallback: aggregate manually if no Overall rows
      const cityMap2 = {}
      data.forEach(r => {
        if (!r.city) return
        if (!cityMap2[r.city]) cityMap2[r.city] = {city:r.city,total_orders:0,total_drops:0,completed_orders:0,failed_orders:0,total_volume_cbm:0,_seenCats:new Set()}
        if (!cityMap2[r.city]._seenCats.has(r.analysis_category)) {
          cityMap2[r.city].total_orders += r.total_orders||0
          cityMap2[r.city].total_drops += r.total_drops||0
          cityMap2[r.city].completed_orders += r.completed_orders||0
          cityMap2[r.city].failed_orders += r.failed_orders||0
          cityMap2[r.city].total_volume_cbm += parseFloat(r.total_volume_cbm||0)
          cityMap2[r.city]._seenCats.add(r.analysis_category)
        }
      })
      rows = Object.values(cityMap2).map(r => ({
        ...r,
        rejection_pct: r.total_orders>0 ? parseFloat((r.failed_orders/r.total_orders*100).toFixed(2)) : 0
      }))
    }
  } else {
    rows = data.filter(r => r.analysis_category === category)
  }
  rows = rows.sort((a,b) => (b.total_drops||0) - (a.total_drops||0))
  if (!rows.length) return <div style={{color:'#73726c',fontSize:13}}>No data for this category.</div>
  return (
    <Tbl
      cols={['City','Orders','Drops','Completed','Failed','Vol CBM','Rej %']}
      colWidths={[140,80,80,90,80,90,80]}
      rows={rows.map(r=>[
        <span style={{fontWeight:500}}>{r.city}</span>,
        num(r.total_orders), num(r.total_drops),
        num(r.completed_orders), num(r.failed_orders),
        dec(r.total_volume_cbm,2),
        <span style={{color:rclr(r.rejection_pct)}}>{pct(r.rejection_pct)}</span>,
      ])}
    />
  )
}

// ── Main Dashboard ────────────────────────────────────────────────────────────
export default function Dashboard() {
  const [activeTab, setTab]         = useState('daily')
  const [dates, setDates]           = useState([])
  const [selectedDate, setDate]     = useState('')
  const [selectedMonth, setMonth]   = useState('')
  const [dailyData, setDailyData]   = useState(null)
  const [mtdData, setMtdData]       = useState(null)
  const [ytdData, setYtdData]       = useState(null)
  const [emiratesData, setEmirates] = useState(null)
  const [rdData, setRD]             = useState(null)
  const [loading, setLoading]       = useState(false)
  const [uploading, setUploading]   = useState(false)
  const [uploadMsg, setUploadMsg]   = useState('')
  const [tourFilter, setTourFilter] = useState('all')
  const [emiratesCat, setEmiCat]    = useState('Overall')
  const [exclusions, setExclusions] = useState([])
  const [newExcl, setNewExcl]       = useState('')
  const [vmMsg, setVmMsg]           = useState('')
  const [vmSummary, setVmSummary]   = useState(null)
  const [vmVersions, setVmVersions] = useState([])
  const [backupHistory, setBH]      = useState([])
  const [backingUp, setBackingUp]   = useState(false)
  const [settings, setSettings]     = useState({})
  const vehicleFileRef  = useRef()
  const modelFileRef    = useRef()

  // Load dates on mount
  useEffect(() => {
    fetch('/api/analytics?view=dates').then(r=>r.json()).then(d=>{
      setDates(d.dates||[])
      if (d.dates?.length) setDate(d.dates[0].dispatch_date)
    })
    loadVehicleMaster()
  }, [])

  // Load daily data when date changes
  useEffect(() => {
    if (!selectedDate) return
    setLoading(true)
    fetch(`/api/analytics?view=daily&date=${selectedDate}&category=Overall`)
      .then(r=>r.json()).then(d=>{ setDailyData(d); setLoading(false) })
      .catch(()=>setLoading(false))
  }, [selectedDate])

  // Load MTD when month changes or tab switches
  useEffect(() => {
    if (activeTab !== 'mtd' && activeTab !== 'daily') return
    const month = selectedDate ? selectedDate.slice(0,7) : selectedMonth
    if (!month) return
    fetch(`/api/analytics?view=mtd&month=${month}&category=Overall`)
      .then(r=>r.json()).then(d=>setMtdData(d))
  }, [selectedDate, selectedMonth, activeTab])

  // Load YTD
  useEffect(() => {
    if (activeTab !== 'ytd') return
    const year = selectedDate ? selectedDate.slice(0,4) : new Date().getFullYear()
    fetch(`/api/analytics?view=ytd&year=${year}`)
      .then(r=>r.json()).then(d=>setYtdData(d))
  }, [activeTab, selectedDate])

  // Load Emirates
  useEffect(() => {
    if (activeTab !== 'emirates') return
    if (!selectedDate) return
    fetch(`/api/analytics?view=emirates&date=${selectedDate}`)
      .then(r=>r.json()).then(d=>setEmirates(d.emirates||[]))
  }, [activeTab, selectedDate])

  // Load Redelivery
  useEffect(() => {
    if (activeTab !== 'redelivery') return
    fetch('/api/analytics?view=redelivery')
      .then(r=>r.json()).then(d=>setRD(d))
  }, [activeTab])

  const loadVehicleMaster = () => {
    fetch('/api/vehicle-master').then(r=>r.json()).then(d=>{
      setVmSummary(d.summary)
      setVmVersions(d.versions||[])
    })
  }

  const loadExclusions = () => {
    fetch('/api/analytics?view=dates').then(()=>{}) // placeholder
    fetch('/api/analytics?view=dates').then(()=>{})
  }

  const handleUpload = useCallback(async (e) => {
    const file = e.target.files?.[0]; if (!file) return
    setUploading(true); setUploadMsg('Uploading and processing...')
    const fd = new FormData(); fd.append('file', file)
    try {
      const r = await fetch('/api/ingest', { method:'POST', body:fd })
      const d = await r.json()
      if (d.status==='success') {
        setUploadMsg(`✓ ${d.rows_saved} rows saved for ${d.dates?.join(', ')}`)
        const dates = await fetch('/api/analytics?view=dates').then(r=>r.json())
        setDates(dates.dates||[])
        if (d.dates?.[0]) setDate(d.dates[0])
      } else {
        setUploadMsg(`Error: ${d.error}`)
      }
    } catch(err) { setUploadMsg(`Error: ${err.message}`) }
    setUploading(false)
  }, [])

  const handleVehicleUpload = async () => {
    const vf = vehicleFileRef.current?.files?.[0]
    const mf = modelFileRef.current?.files?.[0]
    if (!vf || !mf) { setVmMsg('Please select both vehicle entity and model files'); return }
    setVmMsg('Uploading vehicle master...')
    const fd = new FormData()
    fd.append('vehicle_file', vf); fd.append('model_file', mf)
    fd.append('notes', `Uploaded ${new Date().toLocaleDateString()}`)
    try {
      const r = await fetch('/api/vehicle-master', { method:'POST', body:fd })
      const d = await r.json()
      if (d.status==='success') {
        setVmMsg(`✓ ${d.total_vehicles} vehicles loaded — ${d.own_vehicles} OWN, ${d.dleased_vehicles} D-LEASED, ${d.virtual_excluded} virtual excluded`)
        loadVehicleMaster()
      } else {
        setVmMsg(`Error: ${d.error}`)
      }
    } catch(err) { setVmMsg(`Error: ${err.message}`) }
  }

  const downloadBackup = async () => {
    setBackingUp(true)
    window.open('/api/backup', '_blank')
    setTimeout(() => {
      fetch('/api/backup', {method:'POST'}).then(r=>r.json()).then(d=>setBH(d.history||[]))
      setBackingUp(false)
    }, 3000)
  }

  const cats = ['all','NHC Ambient','NHC Frozen','HC']
  const summary = dailyData?.summary || []
  const tours   = dailyData?.tours   || []

  // MTD daily series for chart
  const mtdDaily = mtdData?.daily || []
  const mtdDates = [...new Set(mtdDaily.map(r=>r.dispatch_date))].sort()

  return (
    <div style={{maxWidth:1200,margin:'0 auto',padding:'1.5rem 1rem',fontFamily:'system-ui,sans-serif',fontSize:14,color:'#1a1a18'}}>

      {/* Top bar */}
      <div style={{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:'1.25rem',flexWrap:'wrap',gap:8}}>
        <div>
          <h1 style={{fontSize:20,fontWeight:600,margin:0}}>Route Pattern Intelligence</h1>
          <div style={{fontSize:12,color:'#73726c',marginTop:2}}>Daily · MTD · YTD · Emirates · Re-deliveries · Settings</div>
        </div>
        <div style={{display:'flex',gap:8,flexWrap:'wrap',alignItems:'center'}}>
          <select value={selectedDate} onChange={e=>setDate(e.target.value)}
            style={{fontSize:12,padding:'5px 10px',borderRadius:8,border:'0.5px solid #ccc',background:'#fff'}}>
            {dates.map(d=><option key={d.dispatch_date} value={d.dispatch_date}>{d.dispatch_date} — {num(d.total_orders)} orders</option>)}
          </select>
          <button onClick={()=>window.open(`/api/export?date=${selectedDate}&format=xlsx`,'_blank')}
            style={{fontSize:12,padding:'5px 14px',borderRadius:8,border:'0.5px solid #ccc',cursor:'pointer',background:'transparent'}}>
            Export Excel
          </button>
          <button onClick={()=>window.open(`/api/export?date=${selectedDate}&format=csv`,'_blank')}
            style={{fontSize:12,padding:'5px 14px',borderRadius:8,border:'0.5px solid #ccc',cursor:'pointer',background:'transparent'}}>
            Export CSV
          </button>
        </div>
      </div>

      {/* Upload zone */}
      <label style={{display:'block',border:'1.5px dashed #ccc',borderRadius:12,padding:'1rem',textAlign:'center',cursor:'pointer',marginBottom:'1.25rem',background:uploading?'#f5f4f0':'transparent'}}>
        <input type="file" accept=".xlsx,.xls,.csv" style={{display:'none'}} onChange={handleUpload} disabled={uploading}/>
        <div style={{fontWeight:500}}>Manual upload — drag & drop or click</div>
        <div style={{fontSize:12,color:'#73726c',marginTop:3}}>Auto-schedule reads from locus-exports@akigroup.com at 6am Mon–Fri</div>
        {uploadMsg && <div style={{fontSize:12,marginTop:6,color:uploadMsg.startsWith('✓')?C.good:C.bad}}>{uploadMsg}</div>}
      </label>

      {/* KPI strip */}
      {summary.length > 0 && (() => {
        const ov = summary.find(r=>r.analysis_category==='Overall') || {}
        return (
          <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(130px,1fr))',gap:10,marginBottom:'1.25rem'}}>
            <KPI label="Total tours"       value={num(ov.included_tours)}          sub="included"/>
            <KPI label="Total drops"       value={num(ov.total_drops)}             sub="unique locations"/>
            <KPI label="Avg drops/route"   value={dec(ov.overall_avg_drops)}       sub="all routes"/>
            <KPI label="Avg excl. single"  value={dec(ov.avg_drops_excl_single)}   sub="multi-drop only"/>
            <KPI label="Single drops"      value={num(ov.single_drop_count)}       sub="1–2 location routes"/>
            <KPI label="Bulk routes"       value={num(ov.bulk_route_count)}        sub="multi + ≥80% util" color={C.purple}/>
            <KPI label="CBM utilisation"   value={pct(ov.avg_volume_util_pct)}     color={clr(ov.avg_volume_util_pct)}/>
            <KPI label="Rejection rate"    value={pct(ov.daily_rejection_pct)}     color={rclr(ov.daily_rejection_pct)}/>
            <KPI label="RD %"             value={pct(ov.rd_pct)}                  color={rclr(ov.rd_pct)} sub="re-deliveries"/>
            <KPI label="First attempt %"   value={pct(ov.first_attempt_success_pct)} color={clr(ov.first_attempt_success_pct)}/>
          </div>
        )
      })()}

      {/* Tabs */}
      <div style={{display:'flex',gap:4,marginBottom:'1.25rem',flexWrap:'wrap'}}>
        {[['daily','Daily'],['mtd','MTD'],['ytd','YTD'],['emirates','Emirates'],['redelivery','Re-deliveries'],['vehicles','Vehicle Master'],['settings','Settings']].map(([k,l])=>(
          <Tab key={k} label={l} active={activeTab===k} onClick={()=>setTab(k)}/>
        ))}
      </div>

      {loading && <div style={{textAlign:'center',color:'#73726c',padding:'2rem'}}>Loading...</div>}

      {/* ── DAILY TAB ────────────────────────────────────────────────────────── */}
      {activeTab==='daily' && !loading && (
        <>
          <MetricMatrix data={summary} title={`Daily metrics — ${selectedDate}`}/>

          <Card>
            <SectionTitle>Tour detail</SectionTitle>
            <div style={{display:'flex',gap:8,marginBottom:12,flexWrap:'wrap'}}>
              {cats.map(c=>(
                <button key={c} onClick={()=>setTourFilter(c)} style={{
                  padding:'4px 12px',fontSize:12,borderRadius:8,cursor:'pointer',
                  border:`0.5px solid ${tourFilter===c?C.blue:'#ccc'}`,
                  background:tourFilter===c?C.bgBlue:'transparent',
                  color:tourFilter===c?C.blue:'#73726c'
                }}>{c==='all'?'All categories':c}</button>
              ))}
            </div>
            <TourTable tours={tours} filter={tourFilter}/>
          </Card>
        </>
      )}

      {/* ── MTD TAB ──────────────────────────────────────────────────────────── */}
      {activeTab==='mtd' && (
        <>
          {mtdData?.summary?.length > 0 && (
            <MetricMatrix data={mtdData.summary} title={`Month-to-date — ${selectedDate?.slice(0,7)}`}/>
          )}
          {mtdDaily.length > 0 && (
            <Card>
              <SectionTitle>Daily trend — drops & rejection %</SectionTitle>
              <div style={{display:'flex',gap:8,marginBottom:12}}>
                <div style={{fontSize:12,color:'#73726c',display:'flex',gap:16,alignItems:'center'}}>
                  <span><span style={{display:'inline-block',width:10,height:10,borderRadius:2,background:C.blue,marginRight:4}}></span>Avg drops</span>
                  <span><span style={{display:'inline-block',width:10,height:3,background:C.bad,marginRight:4}}></span>Rejection %</span>
                </div>
              </div>
              <div style={{position:'relative',height:260}}>
                <Bar
                  data={{
                    labels: mtdDates,
                    datasets:[
                      {label:'Avg drops',data:mtdDates.map(d=>mtdDaily.find(r=>r.dispatch_date===d)?.overall_avg_drops||0),backgroundColor:'#85B7EB',borderColor:C.blue,borderWidth:1,yAxisID:'y'},
                      {label:'Rej %',data:mtdDates.map(d=>mtdDaily.find(r=>r.dispatch_date===d)?.daily_rejection_pct||0),type:'line',borderColor:C.bad,borderWidth:2,pointRadius:4,backgroundColor:'transparent',yAxisID:'y1'},
                    ]
                  }}
                  options={{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{
                    x:{ticks:{maxRotation:45,font:{size:10}}},
                    y:{title:{display:true,text:'Avg drops',font:{size:10}}},
                    y1:{position:'right',title:{display:true,text:'Rej %',font:{size:10}},grid:{display:false}}
                  }}}
                />
              </div>
            </Card>
          )}
          {/* MTD daily table */}
          {mtdDaily.length > 0 && (
            <Card>
              <SectionTitle>Daily breakdown table</SectionTitle>
              <Tbl
                cols={['Date','Tours','Drops','OWN','OWN Drops','D-LEASED','DL Drops','Single','Bulk','CBM%','Rej%','RD%']}
                colWidths={[90,60,65,55,80,75,75,60,55,65,60,60]}
                rows={mtdDaily.map(r=>[
                  r.dispatch_date, num(r.included_tours), num(r.total_drops),
                  num(r.own_vehicles), num(r.own_drops),
                  num(r.dleased_vehicles), num(r.dleased_drops),
                  num(r.single_drop_count), num(r.bulk_route_count),
                  <span style={{color:clr(r.avg_volume_util_pct)}}>{pct(r.avg_volume_util_pct)}</span>,
                  <span style={{color:rclr(r.daily_rejection_pct)}}>{pct(r.daily_rejection_pct)}</span>,
                  <span style={{color:rclr(r.rd_pct)}}>{pct(r.rd_pct)}</span>,
                ])}
              />
            </Card>
          )}
          {!mtdData && <div style={{color:'#73726c',fontSize:13,padding:'1rem 0'}}>Upload dispatch data to see MTD metrics.</div>}
        </>
      )}

      {/* ── YTD TAB ──────────────────────────────────────────────────────────── */}
      {activeTab==='ytd' && (
        <>
          {ytdData?.summary?.length > 0 ? (
            <>
              <Card>
                <SectionTitle>Year-to-date — month by month overview</SectionTitle>
                <div style={{position:'relative',height:280}}>
                  <Bar
                    data={{
                      labels:[...new Set(ytdData.summary.map(r=>r.month_label))],
                      datasets:[
                        {label:'Total drops',data:[...new Set(ytdData.summary.map(r=>r.month_label))].map(m=>ytdData.summary.find(r=>r.month_label===m&&r.analysis_category==='Overall')?.total_drops||0),backgroundColor:'#85B7EB',borderColor:C.blue,borderWidth:1},
                      ]
                    }}
                    options={{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{ticks:{callback:v=>v.toLocaleString()}}}}}
                  />
                </div>
              </Card>
              <Card>
                <SectionTitle>YTD summary by category</SectionTitle>
                <Tbl
                  cols={['Month','Category','Tours','Drops','Avg Drops','Excl Single','Single','OWN drops','DL drops','CBM%','Rej%','RD%']}
                  colWidths={[80,110,60,65,80,90,60,80,75,65,60,60]}
                  rows={ytdData.summary.map(r=>[
                    r.month_label, r.analysis_category,
                    num(r.total_tours), num(r.total_drops),
                    dec(r.overall_avg_drops), dec(r.avg_drops_excl_single),
                    num(r.single_drop_count), num(r.own_drops), num(r.dleased_drops),
                    <span style={{color:clr(r.avg_cbm_util_pct)}}>{pct(r.avg_cbm_util_pct)}</span>,
                    <span style={{color:rclr(r.ytd_rejection_pct)}}>{pct(r.ytd_rejection_pct)}</span>,
                    <span style={{color:rclr(r.ytd_rd_pct)}}>{pct(r.ytd_rd_pct)}</span>,
                  ])}
                />
              </Card>
            </>
          ) : <div style={{color:'#73726c',fontSize:13,padding:'1rem 0'}}>No YTD data available yet. Upload more daily files to build the year view.</div>}
        </>
      )}

      {/* ── EMIRATES TAB ─────────────────────────────────────────────────────── */}
      {activeTab==='emirates' && (
        <Card>
          <SectionTitle>Emirates-wise breakdown — {selectedDate}</SectionTitle>
          <div style={{display:'flex',gap:8,marginBottom:12,flexWrap:'wrap'}}>
            {['Overall','NHC Ambient','NHC Frozen','HC'].map(c=>(
              <button key={c} onClick={()=>setEmiCat(c)} style={{
                padding:'4px 12px',fontSize:12,borderRadius:8,cursor:'pointer',
                border:`0.5px solid ${emiratesCat===c?C.blue:'#ccc'}`,
                background:emiratesCat===c?C.bgBlue:'transparent',
                color:emiratesCat===c?C.blue:'#73726c'
              }}>{c}</button>
            ))}
          </div>
          {emiratesData ? <EmiratesTable data={emiratesData} category={emiratesCat}/> : <div style={{color:'#73726c',fontSize:13}}>Loading...</div>}
          {/* Bar chart */}
          {emiratesData?.length > 0 && (
            <div style={{marginTop:'1rem',position:'relative',height:240}}>
              <Bar
                data={{
                  labels: (emiratesCat==='Overall' ? emiratesData.filter(r=>r.analysis_category==='Overall') : emiratesData.filter(r=>r.analysis_category===emiratesCat)).sort((a,b)=>b.total_drops-a.total_drops).map(r=>r.city),
                  datasets:[
                    {label:'Drops',data:(emiratesCat==='Overall' ? emiratesData.filter(r=>r.analysis_category==='Overall') : emiratesData.filter(r=>r.analysis_category===emiratesCat)).sort((a,b)=>b.total_drops-a.total_drops).map(r=>r.total_drops),backgroundColor:'#9FE1CB',borderColor:C.good,borderWidth:1},
                    {label:'Orders',data:(emiratesCat==='Overall' ? emiratesData.filter(r=>r.analysis_category==='Overall') : emiratesData.filter(r=>r.analysis_category===emiratesCat)).sort((a,b)=>b.total_drops-a.total_drops).map(r=>r.total_orders),backgroundColor:'#85B7EB',borderColor:C.blue,borderWidth:1},
                  ]
                }}
                options={{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:true,position:'top'}},scales:{x:{ticks:{font:{size:10}}}}}}
              />
            </div>
          )}
        </Card>
      )}

      {/* ── RE-DELIVERIES TAB ────────────────────────────────────────────────── */}
      {activeTab==='redelivery' && (
        <>
          {rdData?.stats && (
            <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(150px,1fr))',gap:10,marginBottom:'1rem'}}>
              <KPI label="Total re-deliveries" value={num(rdData.stats.redelivery_count)} color={C.warn}/>
              <KPI label="Avg attempts" value={dec(rdData.stats.avg_attempts)} sub="per re-delivered order"/>
              <KPI label="First attempt success" value={num(rdData.stats.first_attempt_success)} color={C.good}/>
              <KPI label="Total tracked orders" value={num(rdData.stats.total_tracked)}/>
            </div>
          )}
          <Card>
            <SectionTitle>Orders requiring re-delivery</SectionTitle>
            {rdData?.redeliveries?.length > 0 ? (
              <Tbl
                cols={['Task ID','Attempts','First attempt','Last attempt','Days to deliver','Final status','Delivered']}
                colWidths={[180,70,110,110,120,110,80]}
                rows={rdData.redeliveries.map(r=>[
                  <span style={{fontFamily:'monospace',fontSize:11}}>{r.task_id}</span>,
                  <span style={{color:r.total_attempts>2?C.bad:C.warn,fontWeight:500}}>{r.total_attempts}</span>,
                  r.first_attempt_date, r.last_attempt_date,
                  <span style={{color:r.days_to_deliver>3?C.bad:C.warn}}>{r.days_to_deliver} days</span>,
                  r.final_status,
                  r.was_delivered ? <Pill text="Yes" color={C.good} bg={C.bgGood}/> : <Pill text="No" color={C.bad} bg={C.bgBad}/>,
                ])}
              />
            ) : <div style={{color:'#73726c',fontSize:13}}>No re-deliveries found yet. Data builds as you upload more daily files.</div>}
          </Card>
        </>
      )}

      {/* ── VEHICLE MASTER TAB ───────────────────────────────────────────────── */}
      {activeTab==='vehicles' && (
        <>
          {vmSummary && (
            <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(140px,1fr))',gap:10,marginBottom:'1rem'}}>
              <KPI label="Total vehicles"  value={num(vmSummary.total)}/>
              <KPI label="OWN vehicles"    value={num(vmSummary.own)}    color={C.good}/>
              <KPI label="D-LEASED"        value={num(vmSummary.dleased)} color={C.warn}/>
              <KPI label="Virtual/excluded" value={num(vmSummary.virtual)} color={C.gray}/>
              <KPI label="Last updated"    value={vmSummary.last_updated ? new Date(vmSummary.last_updated).toLocaleDateString() : '—'}/>
            </div>
          )}
          <Card>
            <SectionTitle>Upload new vehicle master</SectionTitle>
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:12,marginBottom:12}}>
              <div>
                <div style={{fontSize:12,color:'#73726c',marginBottom:4}}>Vehicle entity CSV (vehicle-entity-...csv)</div>
                <input type="file" accept=".csv" ref={vehicleFileRef} style={{fontSize:12,width:'100%'}}/>
              </div>
              <div>
                <div style={{fontSize:12,color:'#73726c',marginBottom:4}}>Vehicle model CSV (vehicle_model-entity-...csv)</div>
                <input type="file" accept=".csv" ref={modelFileRef} style={{fontSize:12,width:'100%'}}/>
              </div>
            </div>
            <button onClick={handleVehicleUpload} style={{
              padding:'8px 20px',borderRadius:8,border:`0.5px solid ${C.blue}`,
              background:C.bgBlue,color:C.blue,cursor:'pointer',fontSize:13,fontWeight:500
            }}>Upload & Replace Master</button>
            {vmMsg && <div style={{fontSize:12,marginTop:8,color:vmMsg.startsWith('✓')?C.good:C.bad}}>{vmMsg}</div>}
            <div style={{fontSize:11,color:'#9c9a92',marginTop:8}}>
              Each upload replaces the current master and archives the previous version. Old version is never deleted.
            </div>
          </Card>
          {vmVersions.length > 0 && (
            <Card>
              <SectionTitle>Upload history</SectionTitle>
              <Tbl
                cols={['Version','Uploaded','Vehicles','Models','Notes','Active']}
                colWidths={[70,160,80,80,200,70]}
                rows={vmVersions.map(v=>[
                  `v${v.id}`,
                  new Date(v.uploaded_at).toLocaleString(),
                  num(v.vehicle_count), num(v.model_count),
                  v.notes||'—',
                  v.is_active ? <Pill text="Active" color={C.good} bg={C.bgGood}/> : <Pill text="Archived" color={C.gray} bg="#f5f4f0"/>,
                ])}
              />
            </Card>
          )}
        </>
      )}

      {/* ── SETTINGS TAB ─────────────────────────────────────────────────────── */}
      {activeTab==='settings' && (
        <>
          {/* Backup */}
          <Card>
            <SectionTitle>Database backup</SectionTitle>
            <div style={{display:'flex',alignItems:'center',gap:16,flexWrap:'wrap',marginBottom:12}}>
              <button onClick={downloadBackup} disabled={backingUp} style={{
                padding:'8px 20px',borderRadius:8,border:`0.5px solid ${C.good}`,
                background:C.bgGood,color:C.good,cursor:'pointer',fontSize:13,fontWeight:500
              }}>{backingUp?'Preparing backup...':'Download full backup (Excel)'}</button>
              <div style={{fontSize:12,color:'#73726c'}}>
                Downloads all tables: raw tasks, tour metrics, daily summaries, vehicle master, attempt history.
                Run weekly and save to OneDrive.
              </div>
            </div>
            <div style={{fontSize:11,color:'#9c9a92'}}>
              Auto-backup runs every Sunday via cron. Your data is also safe in Neon Postgres — clearing browser cache/cookies never affects it.
            </div>
          </Card>

          {/* Route exclusions */}
          <Card>
            <SectionTitle>Route exclusions</SectionTitle>
            <div style={{fontSize:12,color:'#73726c',marginBottom:12}}>
              Routes matching these patterns are excluded from all KPI calculations (drops, utilisation, averages).
            </div>
            <div style={{display:'flex',gap:8,marginBottom:12}}>
              <input
                value={newExcl}
                onChange={e=>setNewExcl(e.target.value)}
                placeholder="Pattern e.g. SELF_DIC or FIXED_"
                style={{flex:1,fontSize:12,padding:'6px 10px',borderRadius:8,border:'0.5px solid #ccc'}}
              />
              <button onClick={async()=>{
                if (!newExcl.trim()) return
                await fetch('/api/settings/exclusions',{method:'POST',body:JSON.stringify({pattern:newExcl.trim()}),headers:{'Content-Type':'application/json'}})
                setNewExcl('')
              }} style={{padding:'6px 16px',borderRadius:8,border:`0.5px solid ${C.blue}`,background:C.bgBlue,color:C.blue,cursor:'pointer',fontSize:12}}>
                Add exclusion
              </button>
            </div>
            <div style={{fontSize:11,color:'#9c9a92',marginTop:4}}>
              Default exclusions pre-loaded: SELF_DIC, SELF_HC, Self_route, SELF_ (managed in Neon database)
            </div>
          </Card>

          {/* Data health */}
          <Card>
            <SectionTitle>Data health</SectionTitle>
            <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(180px,1fr))',gap:10}}>
              <KPI label="Dates loaded" value={num(dates.length)} sub="dispatch days in database"/>
              <KPI label="Earliest date" value={dates.length?dates[dates.length-1]?.dispatch_date:'—'}/>
              <KPI label="Latest date" value={dates.length?dates[0]?.dispatch_date:'—'}/>
              <KPI label="Vehicle master" value={vmSummary?.total?`${num(vmSummary.total)} vehicles`:'Not loaded'} color={vmSummary?.total?C.good:C.bad}/>
            </div>
          </Card>

          {/* How it works */}
          <Card>
            <SectionTitle>How the system works</SectionTitle>
            <div style={{fontSize:12,color:'#73726c',lineHeight:1.8}}>
              <b style={{color:'#1a1a18'}}>Data safety:</b> Your code lives on GitHub, your data lives in Neon Postgres. Clearing browser cache or cookies does not affect either. Vercel keeps every previous deployment — if something breaks, you can roll back in 10 seconds from the Vercel dashboard.<br/><br/>
              <b style={{color:'#1a1a18'}}>Auto-schedule:</b> Every weekday at 6am UTC, the system checks locus-exports@akigroup.com for new dispatch Excel files. If found, it parses the file, runs the full KPI engine, and updates all tables automatically.<br/><br/>
              <b style={{color:'#1a1a18'}}>Vehicle master:</b> Upload new CSVs anytime from the Vehicle Master tab. Old versions are archived — never deleted.<br/><br/>
              <b style={{color:'#1a1a18'}}>Exclusions:</b> Add route patterns to exclude from calculations. Changes apply immediately to all future KPI calculations.<br/><br/>
              <b style={{color:'#1a1a18'}}>Re-deliveries:</b> The system tracks every task ID across dates. If the same task ID appears on multiple days, it counts as re-delivery attempts automatically.
            </div>
          </Card>
        </>
      )}

      <div style={{marginTop:'1.5rem',fontSize:11,color:'#9c9a92',textAlign:'center'}}>
        Route Pattern Intelligence · Data from {dates.length?`${dates[dates.length-1]?.dispatch_date} to ${dates[0]?.dispatch_date}`:'no data yet'} · Auto-ingests from locus-exports@akigroup.com
      </div>
    </div>
  )
}
