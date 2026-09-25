import { AnimatePresence, motion } from 'framer-motion'
import type { CheckResult } from '../api'
import { stageStyle } from '../stageStyles'

type RowState = CheckResult & { running?: boolean }

type Props = {
  rows: RowState[]
}

function StatusPill({ row }: { row: RowState }) {
  if (row.running) {
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-white/10 px-2.5 py-1 text-xs text-white/70">
        <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-sky-400" />
        running
      </span>
    )
  }
  if (row.status === 'needs_target') {
    return (
      <span className="rounded-full bg-white/10 px-2.5 py-1 text-xs text-white/50">needs target</span>
    )
  }
  if (row.status === 'error') {
    return <span className="rounded-full bg-rose-500/15 px-2.5 py-1 text-xs text-rose-300">error</span>
  }
  return row.passed ? (
    <span className="rounded-full bg-emerald-500/15 px-2.5 py-1 text-xs text-emerald-300">pass</span>
  ) : (
    <span className="rounded-full bg-rose-500/15 px-2.5 py-1 text-xs text-rose-300">fail</span>
  )
}

export default function RequirementList({ rows }: Props) {
  return (
    <div className="scrollbar-thin max-h-[32rem] space-y-2 overflow-y-auto pr-1">
      <AnimatePresence initial={false}>
        {rows.map((row) => {
          const s = stageStyle(row.stage)
          const pct = row.value === null ? null : Math.round(row.value * 100)
          return (
            <motion.div
              key={row.key}
              layout
              initial={{ opacity: 0, x: -12 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0 }}
              className="rounded-xl border border-white/10 bg-white/[0.02] px-4 py-3"
            >
              <div className="flex items-center justify-between gap-3">
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <span className={`h-1.5 w-1.5 rounded-full ${s.dot}`} />
                    <span className={`text-xs font-medium uppercase tracking-wide ${s.text}`}>{row.stage}</span>
                    <span className="truncate font-mono text-sm text-white">{row.key}</span>
                  </div>
                  <p className="mt-0.5 truncate text-xs text-white/45">{row.detail ?? row.description}</p>
                </div>
                <div className="flex shrink-0 items-center gap-3">
                  {pct !== null && (
                    <span className="font-mono text-sm text-white/80">
                      {pct}% <span className="text-white/30">/ {Math.round(row.threshold * 100)}%</span>
                    </span>
                  )}
                  <StatusPill row={row} />
                </div>
              </div>
            </motion.div>
          )
        })}
      </AnimatePresence>
    </div>
  )
}
