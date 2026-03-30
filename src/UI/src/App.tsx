import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import { AuthProvider } from '@/auth/AuthContext'
import { Layout } from '@/components/Layout'
import { Toaster } from '@/components/ui/toaster'
import { Dashboard } from '@/pages/Dashboard'
import { JobsPage } from '@/pages/JobsPage'
import { SchedulerPage } from '@/pages/SchedulerPage'
import { RunOnceJobPage } from '@/pages/RunOnceJobPage'
import { DatasetConfigPage } from '@/pages/DatasetConfigPage'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      retry: 1,
    },
  },
})

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <BrowserRouter>
          <Routes>
            <Route element={<Layout />}>
              <Route index element={<Dashboard />} />
              <Route path="jobs" element={<JobsPage />} />
              <Route path="jobs/new" element={<RunOnceJobPage />} />
              <Route path="scheduler" element={<SchedulerPage />} />
              <Route path="datasets/:datasetId/config" element={<DatasetConfigPage />} />
            </Route>
          </Routes>
        </BrowserRouter>
        <Toaster />
      </AuthProvider>
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  )
}
