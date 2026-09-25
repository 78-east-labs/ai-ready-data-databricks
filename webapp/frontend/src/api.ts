export type Profile = {
  name: string
  description: string
  count: number
}

export type CheckResult = {
  key: string
  stage: string
  description: string
  threshold: number
  value: number | null
  passed: boolean
  status: 'ok' | 'needs_target' | 'error'
  detail: string | null
}

export type ProgressEvent =
  | { type: 'start'; total: number; profile: string }
  | { type: 'progress'; key: string; stage: string; status: 'running' }
  | ({ type: 'result' } & CheckResult)
  | { type: 'done'; results: CheckResult[] }
  | { type: 'error'; detail: string }

export const STAGES = ['Clean', 'Contextual', 'Consumable', 'Current', 'Correlated', 'Compliant'] as const

export async function getStatus(): Promise<{ mock_mode: boolean }> {
  const res = await fetch('/api/status')
  return res.json()
}

export async function getProfiles(): Promise<Profile[]> {
  const res = await fetch('/api/profiles')
  return res.json()
}

export async function startAssessment(catalog: string, schemaName: string, profile: string): Promise<{ id: string }> {
  const res = await fetch('/api/assessments', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ catalog, schema_name: schemaName, profile }),
  })
  if (!res.ok) throw new Error(`failed to start assessment (${res.status})`)
  return res.json()
}

export function streamAssessment(jobId: string, onEvent: (event: ProgressEvent) => void): () => void {
  const source = new EventSource(`/api/assessments/${jobId}/stream`)
  source.onmessage = (msg) => {
    onEvent(JSON.parse(msg.data))
  }
  return () => source.close()
}
