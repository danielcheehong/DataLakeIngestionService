// ─── Enums ──────────────────────────────────────────────────────────────────

export type DataSourceType = 'SqlServer' | 'Oracle' | 'DotNet'
export type ExtractionType = 'StoredProcedure' | 'Query' | 'Package' | 'CodeGenerator'
export type UploadProviderType = 'FileSystem' | 'AzureBlob' | 'AwsS3' | 'Axway'
export type CompressionCodec = 'Snappy' | 'Gzip' | 'None'

// ─── Source ─────────────────────────────────────────────────────────────────

export interface SourceConfiguration {
  sourceId?: string
  type: DataSourceType
  connectionStringKey: string
  extractionType: ExtractionType
  procedureName?: string
  packageName?: string
  sqlFilePath?: string
  providerName?: string
  parameters?: Record<string, unknown>
  useRefCursor?: boolean
  refCursorName?: string
  commandTimeout?: number
}

// ─── Upload ──────────────────────────────────────────────────────────────────

export interface FileSystemConfig {
  basePath?: string
  relativePath?: string
}

export interface AzureBlobConfig {
  containerName?: string
  blobPath?: string
}

export interface AxwayConfig {
  remotePath?: string
}

export interface UploadConfiguration {
  provider: UploadProviderType
  fileSystemConfig?: FileSystemConfig
  azureBlobConfig?: AzureBlobConfig
  axwayConfig?: AxwayConfig
  overwriteExisting?: boolean
  enableRetry?: boolean
  maxRetries?: number
  keepLocalCopy?: boolean
  localCopyPath?: string
}

// ─── Parquet ─────────────────────────────────────────────────────────────────

export interface ParquetConfiguration {
  fileNamePattern?: string
  compressionCodec?: CompressionCodec
  rowGroupSize?: number
  enableStatistics?: boolean
}

// ─── Transformation ──────────────────────────────────────────────────────────

export interface TransformationConfiguration {
  step: string
  options?: Record<string, unknown>
}

// ─── Notifications / Metadata ────────────────────────────────────────────────

export interface NotificationConfiguration {
  onSuccess?: boolean
  onFailure?: boolean
  channels?: string[]
}

export interface MetadataConfiguration {
  owner?: string
  contact?: string
  tags?: string[]
}

// ─── Root DatasetConfiguration ───────────────────────────────────────────────

export interface DatasetConfiguration {
  datasetId: string
  name?: string
  description?: string
  enabled?: boolean
  cronExpression?: string
  source?: SourceConfiguration
  sources?: SourceConfiguration[]
  transformations?: TransformationConfiguration[]
  parquet?: ParquetConfiguration
  upload?: UploadConfiguration
  notifications?: NotificationConfiguration
  metadata?: MetadataConfiguration
}

// ─── API DTOs ────────────────────────────────────────────────────────────────

export interface ScheduledJobDto {
  jobName: string
  groupName: string
  datasetId: string
  cronExpression: string | null
  nextFireTime: string | null
  previousFireTime: string | null
  state: string
}

export interface SchedulerStatusDto {
  schedulerName: string
  schedulerId: string
  isStarted: boolean
  isShutdown: boolean
  inStandbyMode: boolean
  numberOfJobsExecuted: number
}

export interface JobTriggerResultDto {
  success: boolean
  message: string
  datasetId: string
  triggeredAt: string | null
}

export interface JobRemovalResultDto {
  success: boolean
  message: string
  datasetId: string
}

export interface SchedulerOperationResultDto {
  success: boolean
  message: string
  operation: string
}

export interface RescheduleResultDto {
  success: boolean
  message: string
  jobsScheduled: number
  scheduledDatasets: string[]
  failedDatasets: string[]
}

export interface DatasetConfigUpdateResultDto {
  success: boolean
  message: string
  datasetId: string
  updatedConfig?: DatasetConfiguration
}

// ─── Request types ───────────────────────────────────────────────────────────

export interface DatasetConfigUpdateRequest {
  cronExpression?: string
  parameterUpdates?: Record<string, string>
  uploadProvider?: string
}
