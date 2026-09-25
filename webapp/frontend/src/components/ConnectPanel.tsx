import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import type { Profile } from '../api'

type Props = {
  profiles: Profile[]
  loading: boolean
  onRun: (catalog: string, schema: string, profile: string) => void
}

export default function ConnectPanel({ profiles, loading, onRun }: Props) {
  const [catalog, setCatalog] = useState('prod_analytics')
  const [schema, setSchema] = useState('customer_360')
  const [profile, setProfile] = useState('scan')

  useEffect(() => {
    if (!profile && profiles.length) setProfile(profiles[0].name)
  }, [profiles, profile])

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, ease: 'easeOut' }}
      className="mx-auto w-full max-w-2xl"
    >
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-8 shadow-2xl shadow-black/40 backdrop-blur">
        <h2 className="text-lg font-semibold text-white">Scope the assessment</h2>
        <p className="mt-1 text-sm text-white/50">
          Unity Catalog <span className="font-mono text-white/70">catalog.schema</span> to score against a workload profile.
        </p>

        <div className="mt-6 grid grid-cols-2 gap-4">
          <label className="flex flex-col gap-1.5 text-sm text-white/70">
            Catalog
            <input
              value={catalog}
              onChange={(e) => setCatalog(e.target.value)}
              className="rounded-lg border border-white/10 bg-black/30 px-3 py-2 font-mono text-sm text-white outline-none ring-violet-500/40 focus:ring-2"
              placeholder="prod_analytics"
            />
          </label>
          <label className="flex flex-col gap-1.5 text-sm text-white/70">
            Schema
            <input
              value={schema}
              onChange={(e) => setSchema(e.target.value)}
              className="rounded-lg border border-white/10 bg-black/30 px-3 py-2 font-mono text-sm text-white outline-none ring-violet-500/40 focus:ring-2"
              placeholder="customer_360"
            />
          </label>
        </div>

        <div className="mt-6">
          <span className="text-sm text-white/70">Profile</span>
          <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2">
            {profiles.map((p) => (
              <button
                key={p.name}
                onClick={() => setProfile(p.name)}
                className={`rounded-xl border px-4 py-3 text-left transition ${
                  profile === p.name
                    ? 'border-violet-400/60 bg-violet-500/15 shadow-[0_0_0_1px_rgba(167,139,250,0.4)]'
                    : 'border-white/10 bg-white/[0.02] hover:border-white/25 hover:bg-white/[0.05]'
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="font-medium text-white capitalize">{p.name}</span>
                  <span className="rounded-full bg-white/10 px-2 py-0.5 text-xs text-white/60">{p.count} checks</span>
                </div>
                <p className="mt-1 text-xs leading-snug text-white/45 line-clamp-2">{p.description}</p>
              </button>
            ))}
          </div>
        </div>

        <button
          disabled={loading || !catalog || !schema}
          onClick={() => onRun(catalog, schema, profile)}
          className="mt-7 w-full rounded-xl bg-gradient-to-r from-violet-500 to-fuchsia-500 py-3 font-semibold text-white shadow-lg shadow-violet-900/40 transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {loading ? 'Starting…' : 'Run assessment'}
        </button>
      </div>
    </motion.div>
  )
}
