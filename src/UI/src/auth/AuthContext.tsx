import { createContext, useContext, type ReactNode } from 'react'

export interface AuthUser {
  name: string
  email?: string
}

export interface AuthContextValue {
  isAuthenticated: boolean
  user: AuthUser | null
  login: () => void
  logout: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

/**
 * Auth provider — currently a stub that treats all users as authenticated.
 *
 * TODO: Replace the body of this component with an MSAL provider for Azure
 * Entra ID authentication. The `AuthContextValue` interface and `useAuth()`
 * hook contract stay the same, so consumers don't need changes.
 *
 * @example
 * // Future Entra ID wiring:
 * // import { MsalProvider } from '@azure/msal-react'
 * // import { msalInstance } from './msalConfig'
 * // Wrap children with <MsalProvider instance={msalInstance}> and populate
 * // isAuthenticated / user from useMsal() / useAccount() hooks.
 */
export function AuthProvider({ children }: { children: ReactNode }) {
  const value: AuthContextValue = {
    isAuthenticated: true,
    user: { name: 'Dev User', email: 'dev@company.com' },
    login: () => {
      // TODO: call msalInstance.loginRedirect() or loginPopup()
    },
    logout: () => {
      // TODO: call msalInstance.logoutRedirect()
    },
  }

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used inside <AuthProvider>')
  return ctx
}
