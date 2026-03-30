import { useQuery } from '@tanstack/react-query'
import { Activity, CheckCircle2, Clock, XCircle } from 'lucide-react'
import { getSchedulerStatus } from '@/api/scheduler'
import { getJobs } from '@/api/jobs'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { JobStateBadge } from '@/components/JobStateBadge'

function formatFireTime(iso: string | null): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

export function Dashboard() {
  const { data: status, isLoading: statusLoading } = useQuery({
    queryKey: ['scheduler-status'],
    queryFn: getSchedulerStatus,
    refetchInterval: 15_000,
  })

  const { data: jobs, isLoading: jobsLoading } = useQuery({
    queryKey: ['jobs'],
    queryFn: getJobs,
    refetchInterval: 15_000,
  })

  const schedulerBadge = () => {
    if (!status) return <Badge variant="outline">Unknown</Badge>
    if (status.isShutdown) return <Badge variant="error">Shutdown</Badge>
    if (status.inStandbyMode) return <Badge variant="warning">Standby</Badge>
    if (status.isStarted) return <Badge variant="success">Running</Badge>
    return <Badge variant="outline">Unknown</Badge>
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Dashboard</h1>

      {/* Status cards */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Scheduler</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {statusLoading ? (
              <div className="h-6 w-20 animate-pulse rounded bg-muted" />
            ) : (
              schedulerBadge()
            )}
            {status && (
              <p className="mt-1 text-xs text-muted-foreground">{status.schedulerName}</p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Jobs Executed</CardTitle>
            <CheckCircle2 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {statusLoading ? (
              <div className="h-8 w-12 animate-pulse rounded bg-muted" />
            ) : (
              <div className="text-3xl font-bold">{status?.numberOfJobsExecuted ?? 0}</div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Scheduled Jobs</CardTitle>
            <Clock className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {jobsLoading ? (
              <div className="h-8 w-12 animate-pulse rounded bg-muted" />
            ) : (
              <div className="text-3xl font-bold">{jobs?.length ?? 0}</div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Paused Jobs</CardTitle>
            <XCircle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {jobsLoading ? (
              <div className="h-8 w-12 animate-pulse rounded bg-muted" />
            ) : (
              <div className="text-3xl font-bold">
                {jobs?.filter((j) => j.state === 'Paused').length ?? 0}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Job summary table */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Scheduled Jobs</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {jobsLoading ? (
            <div className="space-y-2 p-6">
              {[1, 2, 3].map((i) => (
                <div key={i} className="h-8 animate-pulse rounded bg-muted" />
              ))}
            </div>
          ) : !jobs?.length ? (
            <p className="p-6 text-sm text-muted-foreground">No jobs scheduled.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/40 text-left text-xs font-medium text-muted-foreground">
                    <th className="px-4 py-3">Dataset ID</th>
                    <th className="px-4 py-3">State</th>
                    <th className="px-4 py-3">Cron</th>
                    <th className="px-4 py-3">Next Fire</th>
                    <th className="px-4 py-3">Last Fire</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.map((job) => (
                    <tr key={job.jobName} className="border-b last:border-0 hover:bg-muted/20">
                      <td className="px-4 py-3 font-mono text-xs">{job.datasetId}</td>
                      <td className="px-4 py-3">
                        <JobStateBadge state={job.state} />
                      </td>
                      <td className="px-4 py-3 font-mono text-xs">{job.cronExpression ?? '—'}</td>
                      <td className="px-4 py-3">{formatFireTime(job.nextFireTime)}</td>
                      <td className="px-4 py-3">{formatFireTime(job.previousFireTime)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
