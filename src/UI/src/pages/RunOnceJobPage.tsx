import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { addRunOnceJob } from '@/api/jobs'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { toast } from '@/hooks/use-toast'
import type { DatasetConfiguration, JobTriggerResultDto } from '@/types/api'

const TEMPLATE: DatasetConfiguration = {
  datasetId: 'my-dataset-id',
  name: 'My Dataset',
  description: 'Description of the dataset',
  enabled: true,
  source: {
    type: 'SqlServer',
    connectionStringKey: 'TradesSqlServer',
    extractionType: 'StoredProcedure',
    procedureName: 'dbo.sp_GetData',
    parameters: {
      StartDate: '2024-01-01',
      EndDate: '2025-12-31',
    },
    commandTimeout: 300,
  },
  transformations: [],
  parquet: {
    fileNamePattern: '{datasetId}_{date:yyyyMMdd}_{time:HHmmss}.parquet',
    compressionCodec: 'Snappy',
    rowGroupSize: 10000,
    enableStatistics: true,
  },
  upload: {
    provider: 'FileSystem',
    fileSystemConfig: {
      relativePath: 'MyData/Output/',
    },
    overwriteExisting: false,
    enableRetry: true,
    maxRetries: 3,
  },
}

export function RunOnceJobPage() {
  const [json, setJson] = useState(() => JSON.stringify(TEMPLATE, null, 2))
  const [parseError, setParseError] = useState<string | null>(null)
  const [result, setResult] = useState<JobTriggerResultDto | null>(null)

  const mutation = useMutation({
    mutationFn: addRunOnceJob,
    onSuccess: (res) => {
      setResult(res)
      toast({
        variant: res.success ? 'success' : 'destructive',
        title: res.success ? 'Run-once job submitted' : 'Submission failed',
        description: res.message,
      })
    },
    onError: (err: Error) => {
      toast({ variant: 'destructive', title: 'Request failed', description: err.message })
    },
  })

  function handleSubmit() {
    setParseError(null)
    setResult(null)
    let config: DatasetConfiguration
    try {
      config = JSON.parse(json) as DatasetConfiguration
    } catch (e) {
      setParseError((e as SyntaxError).message)
      return
    }
    if (!config.datasetId?.trim()) {
      setParseError('datasetId is required.')
      return
    }
    mutation.mutate(config)
  }

  function handleReset() {
    setJson(JSON.stringify(TEMPLATE, null, 2))
    setParseError(null)
    setResult(null)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Run Once</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Submit a one-off dataset configuration. A temporary Quartz job will be created, executed
          immediately, and self-removed. The original scheduled job (if any) is unaffected.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Dataset Configuration (JSON)</CardTitle>
          <CardDescription>
            Paste or edit the dataset configuration below. The{' '}
            <code className="rounded bg-muted px-1 text-xs">datasetId</code> field is required.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="config-json">Configuration JSON</Label>
            <Textarea
              id="config-json"
              value={json}
              onChange={(e) => {
                setJson(e.target.value)
                setParseError(null)
              }}
              className="min-h-[400px] font-mono text-xs"
              spellCheck={false}
            />
            {parseError && (
              <p className="text-xs text-destructive">JSON error: {parseError}</p>
            )}
          </div>

          <div className="flex gap-3">
            <Button onClick={handleSubmit} disabled={mutation.isPending}>
              {mutation.isPending ? 'Submitting…' : 'Submit Job'}
            </Button>
            <Button variant="outline" onClick={handleReset}>
              Reset to template
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Result */}
      {result && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              Result
              <Badge variant={result.success ? 'success' : 'error'}>
                {result.success ? 'Success' : 'Failed'}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm">
            <p>{result.message}</p>
            {result.datasetId && (
              <p className="text-muted-foreground">
                Run-once job ID:{' '}
                <code className="rounded bg-muted px-1 font-mono text-xs">{result.datasetId}</code>
              </p>
            )}
            {result.triggeredAt && (
              <p className="text-muted-foreground">
                Triggered at: {new Date(result.triggeredAt).toLocaleString()}
              </p>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
