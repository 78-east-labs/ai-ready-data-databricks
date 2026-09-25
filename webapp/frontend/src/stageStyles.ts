export const STAGE_COLORS: Record<string, { text: string; bg: string; ring: string; dot: string; hex: string }> = {
  Clean: { text: 'text-emerald-300', bg: 'bg-emerald-500/10', ring: 'ring-emerald-500/30', dot: 'bg-emerald-400', hex: '#34d399' },
  Contextual: { text: 'text-sky-300', bg: 'bg-sky-500/10', ring: 'ring-sky-500/30', dot: 'bg-sky-400', hex: '#38bdf8' },
  Consumable: { text: 'text-violet-300', bg: 'bg-violet-500/10', ring: 'ring-violet-500/30', dot: 'bg-violet-400', hex: '#a78bfa' },
  Current: { text: 'text-amber-300', bg: 'bg-amber-500/10', ring: 'ring-amber-500/30', dot: 'bg-amber-400', hex: '#fbbf24' },
  Correlated: { text: 'text-pink-300', bg: 'bg-pink-500/10', ring: 'ring-pink-500/30', dot: 'bg-pink-400', hex: '#f472b6' },
  Compliant: { text: 'text-rose-300', bg: 'bg-rose-500/10', ring: 'ring-rose-500/30', dot: 'bg-rose-400', hex: '#fb7185' },
}

export function stageStyle(stage: string) {
  return STAGE_COLORS[stage] ?? STAGE_COLORS.Clean
}
