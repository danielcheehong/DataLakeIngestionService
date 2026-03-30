import { apiFetch } from './client'
import type { DatasetConfigUpdateRequest, DatasetConfigUpdateResultDto } from '@/types/api'

export const updateDatasetConfig = (
  datasetId: string,
  request: DatasetConfigUpdateRequest,
): Promise<DatasetConfigUpdateResultDto> =>
  apiFetch<DatasetConfigUpdateResultDto>(`/api/datasets/${encodeURIComponent(datasetId)}/config`, {
    method: 'PATCH',
    body: JSON.stringify(request),
  })
