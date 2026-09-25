import { Radar, RadarChart, PolarAngleAxis, PolarGrid, PolarRadiusAxis, ResponsiveContainer } from 'recharts'
import type { CheckResult } from '../api'
import { STAGES } from '../api'

type Props = {
  results: CheckResult[]
}

export default function FactorRadar({ results }: Props) {
  const data = STAGES.map((stage) => {
    const rows = results.filter((r) => r.stage === stage && r.value !== null)
    const avg = rows.length ? rows.reduce((sum, r) => sum + (r.value ?? 0), 0) / rows.length : 0
    return { stage, score: Math.round(avg * 100) }
  })

  return (
    <div className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <RadarChart data={data} outerRadius="75%">
          <PolarGrid stroke="rgba(255,255,255,0.12)" />
          <PolarAngleAxis dataKey="stage" tick={{ fill: 'rgba(255,255,255,0.65)', fontSize: 12 }} />
          <PolarRadiusAxis angle={30} domain={[0, 100]} tick={{ fill: 'rgba(255,255,255,0.3)', fontSize: 10 }} />
          <Radar name="Readiness" dataKey="score" stroke="#a78bfa" fill="#a78bfa" fillOpacity={0.35} />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  )
}
