import { useEffect, useState, useCallback } from "react";
import axios from "axios";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer,
  BarChart, Bar, Legend, PieChart, Pie, Cell
} from "recharts";

const API = "http://127.0.0.1:5000";
const COLORS = ["#6366f1","#10b981","#f59e0b","#ef4444"];
const REGIONS = ["All","North","South","East","West"];

function StatCard({ label, value, color, unit = "" }) {
  return (
    <div style={{
      background: color, borderRadius: 12, padding: "18px 24px",
      minWidth: 140, boxShadow: "0 4px 12px rgba(0,0,0,0.15)", color: "#fff"
    }}>
      <div style={{ fontSize: 13, opacity: 0.85, marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 700 }}>{value}{unit}</div>
    </div>
  );
}

function SectionTitle({ children }) {
  return (
    <h3 style={{ color: "#334155", borderLeft: "4px solid #6366f1",
                 paddingLeft: 12, marginTop: 36, marginBottom: 16 }}>
      {children}
    </h3>
  );
}

function AlertBadge({ count }) {
  if (!count) return null;
  return (
    <span style={{ background:"#ef4444", color:"#fff", borderRadius:12,
                   padding:"2px 10px", fontSize:12, marginLeft:8, fontWeight:700 }}>
      {count} Critical
    </span>
  );
}

function App() {
  const [data, setData]             = useState([]);
  const [regionData, setRegionData] = useState([]);
  const [summary, setSummary]       = useState({});
  const [alerts, setAlerts]         = useState([]);
  const [hourlyTrend, setHourly]    = useState([]);
  const [selectedRegion, setRegion] = useState("All");
  const [loading, setLoading]       = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [error, setError]           = useState(null);

  const fetchAll = useCallback(async () => {
    try {
      const regionParam = selectedRegion !== "All" ? `?region=${selectedRegion}` : "";
      const [d, r, s, a, h] = await Promise.all([
        axios.get(`${API}/data${regionParam}`),
        axios.get(`${API}/region`),
        axios.get(`${API}/summary`),
        axios.get(`${API}/alerts`),
        axios.get(`${API}/trend/hourly`),
      ]);
      setData(d.data);
      setRegionData(r.data);
      setSummary(s.data);
      setAlerts(a.data);
      setHourly(h.data);
      setLastUpdated(new Date().toLocaleTimeString());
      setLoading(false);
      setError(null);
    } catch (e) {
      setError("Backend offline — start the Node.js server (npm start in /backend).");
      setLoading(false);
    }
  }, [selectedRegion]);

  useEffect(() => {
    fetchAll();
    const interval = setInterval(fetchAll, 10000);
    return () => clearInterval(interval);
  }, [fetchAll]);

  if (loading) return (
    <div style={{ display:"flex", alignItems:"center", justifyContent:"center",
                  height:"100vh", flexDirection:"column", gap:16 }}>
      <div style={{ fontSize:36 }}>⚡</div>
      <p style={{ color:"#6366f1", fontWeight:600 }}>Loading Power Grid Analytics...</p>
    </div>
  );

  const pieData = regionData.map((r, i) => ({
    name: r.region, value: parseFloat(r.avg_usage) || 0, fill: COLORS[i % 4]
  }));

  return (
    <div style={{ fontFamily:"'Segoe UI',sans-serif", background:"#f8fafc",
                  minHeight:"100vh", padding:"24px 32px", color:"#1e293b" }}>

      {/* Header */}
      <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between",
                    marginBottom:28, borderBottom:"2px solid #e2e8f0", paddingBottom:16 }}>
        <div>
          <h1 style={{ margin:0, fontSize:28, fontWeight:800, color:"#1e293b" }}>
            ⚡ Power Grid Analytics Dashboard
          </h1>
          <p style={{ margin:"4px 0 0", color:"#64748b", fontSize:13 }}>
            Real-time electricity monitoring · Last updated: {lastUpdated || "–"}
            <AlertBadge count={alerts.length} />
          </p>
        </div>
        <div style={{ display:"flex", alignItems:"center", gap:12 }}>
          <label style={{ color:"#64748b", fontSize:14 }}>Region:</label>
          <select
            value={selectedRegion}
            onChange={e => setRegion(e.target.value)}
            style={{ padding:"8px 14px", borderRadius:8, border:"1px solid #cbd5e1",
                     fontSize:14, cursor:"pointer", background:"#fff" }}>
            {REGIONS.map(r => <option key={r}>{r}</option>)}
          </select>
        </div>
      </div>

      {error && (
        <div style={{ background:"#fef2f2", border:"1px solid #fca5a5", borderRadius:10,
                      padding:"14px 20px", marginBottom:20, color:"#dc2626" }}>
          ⚠️ {error}
        </div>
      )}

      {/* Summary Cards */}
      <div style={{ display:"flex", gap:16, flexWrap:"wrap", marginBottom:8 }}>
        <StatCard label="Total Records"      value={summary.total_records  || data.length} color="#6366f1" />
        <StatCard label="Avg Consumption"    value={summary.avg_consumption || "–"} unit=" kWh" color="#10b981" />
        <StatCard label="Peak Consumption"   value={summary.max_consumption || "–"} unit=" kWh" color="#f59e0b" />
        <StatCard label="Critical Alerts"    value={alerts.length}                   color="#ef4444" />
      </div>

      {/* Line Chart: Consumption over time */}
      <SectionTitle>📈 Electricity Consumption Over Time ({selectedRegion})</SectionTitle>
      <div style={{ background:"#fff", borderRadius:12, padding:"20px 10px",
                    boxShadow:"0 2px 8px rgba(0,0,0,0.07)" }}>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={data.slice().reverse()}>
            <CartesianGrid stroke="#f1f5f9" />
            <XAxis dataKey="timestamp" tick={{ fontSize:10 }}
                   tickFormatter={v => v ? String(v).slice(11,16) : ""} />
            <YAxis unit=" kWh" tick={{ fontSize:11 }} />
            <Tooltip formatter={(v) => [`${v} kWh`, "Consumption"]} />
            <Line type="monotone" dataKey="consumption" stroke="#6366f1"
                  strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Bar + Pie charts side by side */}
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:20, marginTop:4 }}>
        <div>
          <SectionTitle>📊 Region-wise Average Consumption</SectionTitle>
          <div style={{ background:"#fff", borderRadius:12, padding:"20px 10px",
                        boxShadow:"0 2px 8px rgba(0,0,0,0.07)" }}>
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={regionData}>
                <CartesianGrid stroke="#f1f5f9" />
                <XAxis dataKey="region" />
                <YAxis unit=" kWh" tick={{ fontSize:11 }} />
                <Tooltip formatter={v => [`${v} kWh`,"Avg"]} />
                <Bar dataKey="avg_usage" fill="#6366f1" radius={[6,6,0,0]}>
                  {regionData.map((_, i) => <Cell key={i} fill={COLORS[i % 4]} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div>
          <SectionTitle>🍩 Consumption Share by Region</SectionTitle>
          <div style={{ background:"#fff", borderRadius:12, padding:"20px 10px",
                        boxShadow:"0 2px 8px rgba(0,0,0,0.07)", textAlign:"center" }}>
            <ResponsiveContainer width="100%" height={240}>
              <PieChart>
                <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%"
                     outerRadius={90} label={({ name, percent }) =>
                       `${name}: ${(percent*100).toFixed(1)}%`}>
                  {pieData.map((entry, i) => <Cell key={i} fill={entry.fill} />)}
                </Pie>
                <Tooltip formatter={v => [`${v} kWh avg`]} />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Hourly trend */}
      <SectionTitle>🕐 Hourly Consumption Trend (All Regions)</SectionTitle>
      <div style={{ background:"#fff", borderRadius:12, padding:"20px 10px",
                    boxShadow:"0 2px 8px rgba(0,0,0,0.07)" }}>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={hourlyTrend}>
            <CartesianGrid stroke="#f1f5f9" />
            <XAxis dataKey="hour" tickFormatter={h => `${h}:00`} tick={{ fontSize:10 }} />
            <YAxis unit=" kWh" tick={{ fontSize:11 }} />
            <Tooltip formatter={v => [`${v} kWh`,"Avg"]}
                     labelFormatter={h => `Hour ${h}:00`} />
            <Bar dataKey="avg_kwh" fill="#10b981" radius={[4,4,0,0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Alerts Table */}
      {alerts.length > 0 && (
        <>
          <SectionTitle>🚨 Critical Alerts (Consumption &gt; 400 kWh)</SectionTitle>
          <div style={{ background:"#fff", borderRadius:12, overflow:"hidden",
                        boxShadow:"0 2px 8px rgba(0,0,0,0.07)" }}>
            <table style={{ width:"100%", borderCollapse:"collapse", fontSize:13 }}>
              <thead>
                <tr style={{ background:"#fef2f2" }}>
                  {["Timestamp","Region","Consumption","Load Level"].map(h =>
                    <th key={h} style={{ padding:"12px 16px", textAlign:"left",
                                         color:"#dc2626", fontWeight:600 }}>{h}</th>
                  )}
                </tr>
              </thead>
              <tbody>
                {alerts.slice(0,10).map((a, i) => (
                  <tr key={i} style={{ borderTop:"1px solid #fee2e2",
                                       background: i%2===0?"#fff":"#fff7f7" }}>
                    <td style={{ padding:"10px 16px", color:"#64748b" }}>
                      {String(a.timestamp).slice(0,19)}
                    </td>
                    <td style={{ padding:"10px 16px", fontWeight:600 }}>{a.region}</td>
                    <td style={{ padding:"10px 16px", color:"#dc2626", fontWeight:700 }}>
                      {a.consumption} kWh
                    </td>
                    <td style={{ padding:"10px 16px" }}>
                      <span style={{ background:"#fef2f2", color:"#dc2626", padding:"3px 10px",
                                     borderRadius:20, fontSize:11, fontWeight:600 }}>
                        {a.load_level || "Critical"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* Footer */}
      <div style={{ marginTop:40, textAlign:"center", color:"#94a3b8", fontSize:12 }}>
        Power Grid Analytics Dashboard · Capstone Project · Data Engineering · 2026
      </div>
    </div>
  );
}

export default App;
