export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

/**
 * Base fetch wrapper. In dev the Vite proxy strips VITE_API_BASE_URL from the
 * path (it is empty), so requests go to /api/... which the proxy forwards to
 * http://localhost:5080. In production VITE_API_BASE_URL is the full origin of
 * the deployed backend.
 */
export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const base = import.meta.env.VITE_API_BASE_URL ?? ''
  const url = `${base}${path}`

  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  })

  if (!res.ok) {
    let message = res.statusText
    try {
      const body = await res.json()
      message = body?.message ?? body?.error ?? message
    } catch {
      // ignore parse errors
    }
    throw new ApiError(res.status, message)
  }

  // 204 No Content
  if (res.status === 204) return undefined as T

  return res.json() as Promise<T>
}
