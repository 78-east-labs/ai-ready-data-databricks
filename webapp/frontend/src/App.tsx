import { useEffect, useMemo, useState } from 'react'
import { motion } from 'framer-motion'
import { getProfiles, getStatus, startAssessment, streamAssessment } from './api'
import type { CheckResult, Profile, ProgressEvent } from './api'
import ConnectPanel from './components/ConnectPanel'
import ScoreRing from './components/ScoreRing'
import FactorRadar from './components/FactorRadar'
import RequirementList from './components/RequirementList'

type RowState = CheckResult & { running?: boolean }
type Phase = 'form' | 'running' | 'done'

export default function App() {
  const [mockMode, setMockMode] = useState(false)
  const [profiles, setProfiles] = useState<Profile[]>([])
  const [phase, setPhase] = useState<Phase>('form')
  const [target, setTarget] = useState<{ catalog: string; schema: string; profile: string } | null>(null)
  const [total, setTotal] = useState(0)
  const [rows, setRows] = useState<RowState[]>([])
  const [starting, setStarting] = useState(false)

  useEffect(() => {
    getStatus().then((s) => setMockMode(s.mock_mode))
    getProfiles().then(setProfiles)
  }, [])

  const done = rows.filter((r) => !r.running).length
  const overall = useMemo(() => {
    const scored = rows.filter((r) => r.value !== null)
    if (!scored.length) return 0
    return scored.reduce((sum, r) => sum + (r.value ?? 0), 0) / scored.length
  }, [rows])
  const passCount = rows.filter((r) => r.passed).length

  async function handleRun(catalog: string, schema: string, profile: string) {
    setStarting(true)
    try {
      const { id } = await startAssessment(catalog, schema, profile)
      setTarget({ catalog, schema, profile })
      setRows([])
      setTotal(0)
      setPhase('running')

      const close = streamAssessment(id, (event: ProgressEvent) => {
        if (event.type === 'start') {
          setTotal(event.total)
        } else if (event.type === 'progress') {
          setRows((prev) => [...prev, { key: event.key, stage: event.stage, description: '', threshold: 0, value: null, passed: false, status: 'ok', detail: null, running: true }])
        } else if (event.type === 'result') {
          setRows((prev) => prev.map((r) => (r.key === event.key ? { ...event, running: false } : r)))
        } else if (event.type === 'done') {
          setPhase('done')
          close()
        }
      })
    } finally {
      setStarting(false)
    }
  }

  function reset() {
    setPhase('form')
    setTarget(null)
    setRows([])
  }

  return (
    <div className="min-h-full">
      <header className="border-b border-white/5 px-6 py-5">
        <div className="mx-auto flex max-w-6xl items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-violet-500 to-fuchsia-500 text-sm font-bold text-white">
              AI
            </div>
            <div>
              <h1 className="text-sm font-semibold text-white">AI-Ready Data</h1>
              <p className="text-xs text-white/40">Databricks · Unity Catalog readiness</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            {mockMode && (
              <span className="rounded-full border border-amber-400/30 bg-amber-400/10 px-3 py-1 text-xs text-amber-300">
                demo mode — set DATABRICKS_HOST/HTTP_PATH/TOKEN for live checks
              </span>
            )}
            {phase !== 'form' && (
              <button onClick={reset} className="rounded-lg border border-white/10 px-3 py-1.5 text-xs text-white/70 hover:bg-white/5">
                New assessment
              </button>
            )}
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-6xl px-6 py-10">
        {phase === 'form' && <ConnectPanel profiles={profiles} loading={starting} onRun={handleRun} />}

        {phase !== 'form' && target && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="grid grid-cols-1 gap-6 lg:grid-cols-[320px_1fr]">
            <div className="space-y-6">
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-6">
                <p className="font-mono text-sm text-white/70">
                  {target.catalog}.{target.schema}
                </p>
                <p className="mt-1 text-xs uppercase tracking-wide text-white/40">{target.profile} profile</p>
                <div className="mt-6 flex items-center justify-center">
                  <ScoreRing value={overall} label={phase === 'running' ? `${done}/${total}` : 'readiness'} />
                </div>
                <div className="mt-4 flex justify-between text-xs text-white/50">
                  <span>{passCount} passing</span>
                  <span>{rows.length - passCount} gaps</span>
                </div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-6">
                <FactorRadar results={rows} />
              </div>
            </div>

            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-6">
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-sm font-semibold text-white">
                  {phase === 'running' ? 'Running checks…' : 'Results'}
                </h2>
                <span className="text-xs text-white/40">{done}/{total} complete</span>
              </div>
              <RequirementList rows={rows} />
            </div>
          </motion.div>
        )}
      </main>
    </div>
  )
}
