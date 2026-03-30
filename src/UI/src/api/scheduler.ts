import { apiFetch } from './client'
import type { RescheduleResultDto, SchedulerOperationResultDto, SchedulerStatusDto } from '@/types/api'

export const getSchedulerStatus = (): Promise<SchedulerStatusDto> =>
  apiFetch<SchedulerStatusDto>('/api/scheduler/status')

export const pauseAll = (): Promise<SchedulerOperationResultDto> =>
  apiFetch<SchedulerOperationResultDto>('/api/scheduler/pause', { method: 'POST' })

export const resumeAll = (): Promise<SchedulerOperationResultDto> =>
  apiFetch<SchedulerOperationResultDto>('/api/scheduler/resume', { method: 'POST' })

export const rescheduleAll = (): Promise<RescheduleResultDto> =>
  apiFetch<RescheduleResultDto>('/api/scheduler/reschedule', { method: 'POST' })
