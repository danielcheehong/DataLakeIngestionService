import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import { Plus, Trash2, ArrowLeft } from 'lucide-react'
import { updateDatasetConfig } from '@/api/datasets'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { toast } from '@/hooks/use-toast'
import type { DatasetConfigUpdateResultDto, UploadProviderType } from '@/types/api'

interface ParamRow {
  key: string
  value: string
}

const PROVIDERS: UploadProviderType[] = ['FileSystem', 'AzureBlob', 'AwsS3', 'Axway']

export function DatasetConfigPage() {
  const { datasetId } = useParams<{ datasetId: string }>()
  const navigate = useNavigate()

  const [cronExpression, setCronExpression] = useState('')
  const [uploadProvider, setUploadProvider] = useState<string>('')
  const [params, setParams] = useState<ParamRow[]>([{ key: '', value: '' }])
  const [result, setResult] = useState<DatasetConfigUpdateResultDto | null>(null)

  const mutation = useMutation({
    mutationFn: () => {
      const parameterUpdates = Object.fromEntries(
        params
          .filter((r) => r.key.trim())
          .map((r) => [r.key.trim(), r.value]),
      )
      return updateDatasetConfig(datasetId!, {
        ...(cronExpression.trim() && { cronExpression: cronExpression.trim() }),
        ...(Object.keys(parameterUpdates).length > 0 && { parameterUpdates }),
        ...(uploadProvider && { uploadProvider }),
      })
    },
    onSuccess: (res) => {
      setResult(res)
      toast({
        variant: res.success ? 'success' : 'destructive',
        title: res.success ? 'Config updated' : 'Update failed',
        description: res.message,
      })
    },
    onError: (err: Error) => {
      toast({ variant: 'destructive', title: 'Request failed', description: err.message })
    },
  })

  function addParam() {
    setParams((p) => [...p, { key: '', value: '' }])
  }

  function updateParam(idx: number, field: 'key' | 'value', val: string) {
    setParams((p) => p.map((r, i) => (i === idx ? { ...r, [field]: val } : r)))
  }

  function removeParam(idx: number) {
    setParams((p) => p.filter((_, i) => i !== idx))
  }

  const nothingToSave =
    !cronExpression.trim() &&
    !uploadProvider &&
    params.every((r) => !r.key.trim())

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" onClick={() => navigate(-1)}>
          <ArrowLeft className="mr-1 h-4 w-4" />
          Back
        </Button>
        <div>
          <h1 className="text-2xl font-bold">Edit Dataset Config</h1>
          <p className="text-sm text-muted-foreground font-mono">{datasetId}</p>
        </div>
      </div>

      <p className="text-sm text-muted-foreground">
        Only the fields you fill in will be updated. Empty fields are ignored. The backend applies
        changes surgically — unrelated config fields are preserved.
      </p>

      {/* Cron Expression */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Cron Expression</CardTitle>
          <CardDescription>
            Quartz 6-part cron format, e.g.{' '}
            <code className="rounded bg-muted px-1 text-xs">0 0 6 * * ?</code> (every day at 06:00).
            Changing this also live-reschedules the Quartz trigger.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Input
            placeholder="0 0 6 * * ?"
            value={cronExpression}
            onChange={(e) => setCronExpression(e.target.value)}
            className="max-w-xs font-mono"
          />
        </CardContent>
      </Card>

      {/* Parameters */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Source Parameters</CardTitle>
          <CardDescription>
            Override named parameters in{' '}
            <code className="rounded bg-muted px-1 text-xs">source.parameters</code> or{' '}
            <code className="rounded bg-muted px-1 text-xs">sources[*].parameters</code>. Only
            existing keys are updated.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {params.map((row, idx) => (
            <div key={idx} className="flex items-center gap-2">
              <Input
                placeholder="Parameter name"
                value={row.key}
                onChange={(e) => updateParam(idx, 'key', e.target.value)}
                className="font-mono text-sm"
              />
              <Input
                placeholder="Value"
                value={row.value}
                onChange={(e) => updateParam(idx, 'value', e.target.value)}
                className="font-mono text-sm"
              />
              <Button
                variant="ghost"
                size="icon"
                onClick={() => removeParam(idx)}
                disabled={params.length === 1}
                className="shrink-0 text-muted-foreground hover:text-destructive"
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          ))}
          <Button variant="outline" size="sm" onClick={addParam}>
            <Plus className="mr-1 h-3.5 w-3.5" />
            Add parameter
          </Button>
        </CardContent>
      </Card>

      {/* Upload provider */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Upload Provider</CardTitle>
          <CardDescription>
            Switch the destination provider. Leave unselected to keep the existing provider.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Select value={uploadProvider} onValueChange={setUploadProvider}>
            <SelectTrigger className="max-w-xs">
              <SelectValue placeholder="Keep existing" />
            </SelectTrigger>
            <SelectContent>
              {PROVIDERS.map((p) => (
                <SelectItem key={p} value={p}>
                  {p}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </CardContent>
      </Card>

      {/* Save */}
      <div className="flex gap-3">
        <Button onClick={() => mutation.mutate()} disabled={mutation.isPending || nothingToSave}>
          {mutation.isPending ? 'Saving…' : 'Save Changes'}
        </Button>
        <Label className="flex items-center text-xs text-muted-foreground">
          {nothingToSave && 'Fill in at least one field to save.'}
        </Label>
      </div>

      {/* Result */}
      {result && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              Update Result
              <Badge variant={result.success ? 'success' : 'error'}>
                {result.success ? 'Success' : 'Failed'}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <p className="text-sm">{result.message}</p>
            {result.updatedConfig && (
              <div className="space-y-1">
                <p className="text-xs font-medium text-muted-foreground">Updated configuration:</p>
                <pre className="max-h-96 overflow-auto rounded-md border bg-muted p-4 text-xs">
                  {JSON.stringify(result.updatedConfig, null, 2)}
                </pre>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
