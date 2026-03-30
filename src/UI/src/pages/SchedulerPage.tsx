import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { getSchedulerStatus, pauseAll, resumeAll, rescheduleAll } from '@/api/scheduler'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { toast } from '@/hooks/use-toast'

type ConfirmAction = 'pause' | 'resume' | 'reschedule' | null

const ACTION_META: Record<Exclude<ConfirmAction, null>, { label: string; description: string; danger?: boolean }> = {
  pause: {
    label: 'Pause All Jobs',
    description: 'All scheduled jobs will be suspended and will not execute until resumed.',
  },
  resume: {
    label: 'Resume All Jobs',
    description: 'All paused jobs will be resumed and will execute according to their schedules.',
  },
  reschedule: {
    label: 'Reschedule All Jobs',
    description:
      'All existing jobs will be removed, dataset configurations will be reloaded, and jobs will be rescheduled. Any run-once jobs still in queue will be lost.',
    danger: true,
  },
}

function SchedulerBadge({ isStarted, isShutdown, inStandbyMode }: { isStarted: boolean; isShutdown: boolean; inStandbyMode: boolean }) {
  if (isShutdown) return <Badge variant="error">Shutdown</Badge>
  if (inStandbyMode) return <Badge variant="warning">Standby</Badge>
  if (isStarted) return <Badge variant="success">Running</Badge>
  return <Badge variant="outline">Unknown</Badge>
}

export function SchedulerPage() {
  const queryClient = useQueryClient()
  const [confirmAction, setConfirmAction] = useState<ConfirmAction>(null)

  const { data: status, isLoading } = useQuery({
    queryKey: ['scheduler-status'],
    queryFn: getSchedulerStatus,
    refetchInterval: 10_000,
  })

  const actionMutation = useMutation({
    mutationFn: async (action: Exclude<ConfirmAction, null>) => {
      if (action === 'pause') return pauseAll()
      if (action === 'resume') return resumeAll()
      return rescheduleAll()
    },
    onSuccess: (result) => {
      setConfirmAction(null)
      toast({
        variant: result.success ? 'success' : 'destructive',
        title: result.success ? 'Operation succeeded' : 'Operation failed',
        description: result.message,
      })
      void queryClient.invalidateQueries({ queryKey: ['scheduler-status'] })
      void queryClient.invalidateQueries({ queryKey: ['jobs'] })
    },
    onError: (err: Error) => {
      setConfirmAction(null)
      toast({ variant: 'destructive', title: 'Error', description: err.message })
    },
  })

  const meta = confirmAction ? ACTION_META[confirmAction] : null

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Scheduler</h1>

      {/* Status card */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Scheduler Status</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-2">
              {[1, 2, 3, 4].map((i) => <div key={i} className="h-5 animate-pulse rounded bg-muted" />)}
            </div>
          ) : !status ? (
            <p className="text-sm text-muted-foreground">Unable to reach backend.</p>
          ) : (
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm sm:grid-cols-3">
              <div>
                <dt className="text-xs text-muted-foreground">Status</dt>
                <dd className="mt-0.5">
                  <SchedulerBadge
                    isStarted={status.isStarted}
                    isShutdown={status.isShutdown}
                    inStandbyMode={status.inStandbyMode}
                  />
                </dd>
              </div>
              <div>
                <dt className="text-xs text-muted-foreground">Scheduler Name</dt>
                <dd className="mt-0.5 font-mono text-xs">{status.schedulerName}</dd>
              </div>
              <div>
                <dt className="text-xs text-muted-foreground">Instance ID</dt>
                <dd className="mt-0.5 font-mono text-xs truncate">{status.schedulerId}</dd>
              </div>
              <div>
                <dt className="text-xs text-muted-foreground">Jobs Executed</dt>
                <dd className="mt-0.5 text-lg font-bold">{status.numberOfJobsExecuted}</dd>
              </div>
            </dl>
          )}
        </CardContent>
      </Card>

      {/* Controls */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Controls</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-3">
          <Button variant="outline" onClick={() => setConfirmAction('pause')} disabled={actionMutation.isPending}>
            Pause All
          </Button>
          <Button variant="outline" onClick={() => setConfirmAction('resume')} disabled={actionMutation.isPending}>
            Resume All
          </Button>
          <Button
            variant="outline"
            onClick={() => setConfirmAction('reschedule')}
            disabled={actionMutation.isPending}
            className="text-destructive hover:text-destructive"
          >
            Reschedule All
          </Button>
        </CardContent>
      </Card>

      {/* Confirm dialog */}
      <Dialog open={!!confirmAction} onOpenChange={(open) => { if (!open) setConfirmAction(null) }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{meta?.label}</DialogTitle>
            <DialogDescription>{meta?.description}</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmAction(null)}>
              Cancel
            </Button>
            <Button
              variant={meta?.danger ? 'destructive' : 'default'}
              onClick={() => confirmAction && actionMutation.mutate(confirmAction)}
              disabled={actionMutation.isPending}
            >
              {actionMutation.isPending ? 'Working…' : 'Confirm'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
