import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Trash2, Zap, Settings } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { getJobs, removeJob, triggerJob } from '@/api/jobs'
import { JobStateBadge } from '@/components/JobStateBadge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { toast } from '@/hooks/use-toast'

function formatFireTime(iso: string | null) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

export function JobsPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null)

  const { data: jobs, isLoading } = useQuery({
    queryKey: ['jobs'],
    queryFn: getJobs,
    refetchInterval: 15_000,
  })

  const triggerMutation = useMutation({
    mutationFn: triggerJob,
    onSuccess: (result) => {
      toast({
        variant: result.success ? 'success' : 'destructive',
        title: result.success ? 'Job triggered' : 'Trigger failed',
        description: result.message,
      })
      void queryClient.invalidateQueries({ queryKey: ['jobs'] })
    },
    onError: (err: Error) => {
      toast({ variant: 'destructive', title: 'Error', description: err.message })
    },
  })

  const deleteMutation = useMutation({
    mutationFn: removeJob,
    onSuccess: (result) => {
      setDeleteTarget(null)
      toast({
        variant: result.success ? 'success' : 'destructive',
        title: result.success ? 'Job removed' : 'Remove failed',
        description: result.message,
      })
      void queryClient.invalidateQueries({ queryKey: ['jobs'] })
    },
    onError: (err: Error) => {
      setDeleteTarget(null)
      toast({ variant: 'destructive', title: 'Error', description: err.message })
    },
  })

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Jobs</h1>
        <Button onClick={() => navigate('/jobs/new')} size="sm">
          + Run Once
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Scheduled Jobs</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="space-y-2 p-6">
              {[1, 2, 3].map((i) => <div key={i} className="h-10 animate-pulse rounded bg-muted" />)}
            </div>
          ) : !jobs?.length ? (
            <p className="p-6 text-sm text-muted-foreground">No scheduled jobs found.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/40 text-left text-xs font-medium text-muted-foreground">
                    <th className="px-4 py-3">Dataset ID</th>
                    <th className="px-4 py-3">State</th>
                    <th className="px-4 py-3">Cron Expression</th>
                    <th className="px-4 py-3">Next Fire</th>
                    <th className="px-4 py-3">Last Fire</th>
                    <th className="px-4 py-3 text-right">Actions</th>
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
                      <td className="px-4 py-3 text-xs">{formatFireTime(job.nextFireTime)}</td>
                      <td className="px-4 py-3 text-xs">{formatFireTime(job.previousFireTime)}</td>
                      <td className="px-4 py-3">
                        <div className="flex items-center justify-end gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => triggerMutation.mutate(job.datasetId)}
                            disabled={triggerMutation.isPending}
                            title="Trigger now"
                          >
                            <Zap className="h-3.5 w-3.5" />
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => navigate(`/datasets/${encodeURIComponent(job.datasetId)}/config`)}
                            title="Edit config"
                          >
                            <Settings className="h-3.5 w-3.5" />
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setDeleteTarget(job.datasetId)}
                            title="Delete job"
                            className="text-destructive hover:text-destructive"
                          >
                            <Trash2 className="h-3.5 w-3.5" />
                          </Button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Delete confirmation dialog */}
      <Dialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) setDeleteTarget(null) }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Remove job?</DialogTitle>
            <DialogDescription>
              This will permanently remove the scheduled job for{' '}
              <span className="font-mono font-semibold">{deleteTarget}</span>. The job will no longer
              execute. This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteTarget(null)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => deleteTarget && deleteMutation.mutate(deleteTarget)}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending ? 'Removing…' : 'Remove'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
