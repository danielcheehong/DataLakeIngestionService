import { apiFetch } from './client'
import type { DatasetConfiguration, JobRemovalResultDto, JobTriggerResultDto, ScheduledJobDto } from '@/types/api'

export const getJobs = (): Promise<ScheduledJobDto[]> =>
  apiFetch<ScheduledJobDto[]>('/api/jobs')

export const triggerJob = (datasetId: string): Promise<JobTriggerResultDto> =>
  apiFetch<JobTriggerResultDto>(`/api/jobs/${encodeURIComponent(datasetId)}/trigger`, {
    method: 'POST',
  })

export const addRunOnceJob = (config: DatasetConfiguration): Promise<JobTriggerResultDto> =>
  apiFetch<JobTriggerResultDto>('/api/jobs', {
    method: 'POST',
    body: JSON.stringify(config),
  })

export const removeJob = (datasetId: string): Promise<JobRemovalResultDto> =>
  apiFetch<JobRemovalResultDto>(`/api/jobs/${encodeURIComponent(datasetId)}`, {
    method: 'DELETE',
  })
