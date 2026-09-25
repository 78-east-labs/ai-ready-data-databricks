type Props = {
  value: number // 0..1
  label: string
}

export default function ScoreRing({ value, label }: Props) {
  const pct = Math.max(0, Math.min(1, value))
  const radius = 54
  const circumference = 2 * Math.PI * radius
  const offset = circumference * (1 - pct)
  const color = pct >= 0.8 ? '#34d399' : pct >= 0.5 ? '#fbbf24' : '#fb7185'

  return (
    <div className="relative flex h-40 w-40 items-center justify-center">
      <svg width="160" height="160" viewBox="0 0 160 160" className="-rotate-90">
        <circle cx="80" cy="80" r={radius} stroke="rgba(255,255,255,0.08)" strokeWidth="12" fill="none" />
        <circle
          cx="80"
          cy="80"
          r={radius}
          stroke={color}
          strokeWidth="12"
          fill="none"
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          style={{ transition: 'stroke-dashoffset 0.8s cubic-bezier(.4,0,.2,1), stroke 0.4s' }}
        />
      </svg>
      <div className="absolute flex flex-col items-center">
        <span className="text-3xl font-bold text-white">{Math.round(pct * 100)}%</span>
        <span className="text-xs uppercase tracking-wide text-white/50">{label}</span>
      </div>
    </div>
  )
}
