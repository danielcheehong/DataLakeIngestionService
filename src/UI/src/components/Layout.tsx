import { NavLink, Outlet } from 'react-router-dom'
import { Database, LayoutDashboard, List, RefreshCw, Play, User } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useAuth } from '@/auth/AuthContext'

const navItems = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard, end: true },
  { to: '/jobs', label: 'Jobs', icon: List },
  { to: '/scheduler', label: 'Scheduler', icon: RefreshCw },
  { to: '/jobs/new', label: 'Run Once', icon: Play },
]

export function Layout() {
  const { user } = useAuth()

  return (
    <div className="flex h-screen bg-background">
      {/* Sidebar */}
      <aside className="flex w-60 flex-col border-r bg-card">
        {/* Brand */}
        <div className="flex items-center gap-2 border-b px-5 py-4">
          <Database className="h-5 w-5 text-primary" />
          <span className="text-sm font-semibold leading-tight">DataLake<br />Ingestion Service</span>
        </div>

        {/* Nav */}
        <nav className="flex-1 space-y-1 p-3">
          {navItems.map(({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-primary text-primary-foreground'
                    : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
                )
              }
            >
              <Icon className="h-4 w-4" />
              {label}
            </NavLink>
          ))}
        </nav>

        {/* User stub */}
        <div className="border-t p-4">
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <User className="h-4 w-4" />
            <span>{user?.name ?? 'Unknown'}</span>
          </div>
          {/* TODO: Replace with Azure Entra ID account info + logout button */}
        </div>
      </aside>

      {/* Main content */}
      <main className="flex flex-1 flex-col overflow-hidden">
        <div className="flex-1 overflow-y-auto p-6">
          <Outlet />
        </div>
      </main>
    </div>
  )
}
