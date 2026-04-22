using DataLakeIngestionService.Core.Interfaces.DataExtraction;
using DataLakeIngestionService.Core.Models;
using DataLakeIngestionService.Worker.Models;
using DataLakeIngestionService.Worker.Services;
using Microsoft.AspNetCore.Mvc;

namespace DataLakeIngestionService.Worker.Endpoints;

/// <summary>
/// Minimal API endpoints for job and scheduler management.
/// </summary>
public static class JobEndpoints
{
    public static void MapJobEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api")
            .WithTags("Jobs & Scheduler");

        // GET /api/jobs - List all scheduled jobs
        group.MapGet("/jobs", async (IJobManagementService service, CancellationToken ct) =>
        {
            var jobs = await service.GetAllJobsAsync(ct);
            return Results.Ok(jobs);
        })
        .WithName("GetAllJobs")
        .WithSummary("List all scheduled jobs")
        .WithDescription("Returns a list of all scheduled data ingestion jobs with their status and next fire time.");

        // GET /api/scheduler/status - Get scheduler status
        group.MapGet("/scheduler/status", async (IJobManagementService service, CancellationToken ct) =>
        {
            var status = await service.GetSchedulerStatusAsync(ct);
            return Results.Ok(status);
        })
        .WithName("GetSchedulerStatus")
        .WithSummary("Get scheduler status")
        .WithDescription("Returns the current status of the Quartz scheduler.");

        // POST /api/jobs/{datasetId}/trigger - Trigger existing job immediately
        group.MapPost("/jobs/{datasetId}/trigger", async (
            string datasetId,
            IJobManagementService service,
            CancellationToken ct) =>
        {
            var result = await service.TriggerJobAsync(datasetId, ct);
            return result.Success ? Results.Ok(result) : Results.NotFound(result);
        })
        .WithName("TriggerJob")
        .WithSummary("Trigger a job immediately")
        .WithDescription("Triggers an existing scheduled job to run immediately by dataset ID.");

        // POST /api/jobs - Create and execute a temporary run-once job from configuration
        group.MapPost("/jobs", async (
            [FromBody] DatasetConfiguration config,
            IJobManagementService service,
            CancellationToken ct,
            [FromQuery] bool triggerImmediately = true) =>
        {
            if (string.IsNullOrWhiteSpace(config.DatasetId))
                return Results.BadRequest(new { Error = "DatasetId is required." });

            var result = await service.AddAndTriggerJobAsync(config, triggerImmediately, ct);
            return result.Success ? Results.Created($"/api/jobs/{result.DatasetId}", result) : Results.BadRequest(result);
        })
        .WithName("AddJob")
        .WithSummary("Create and execute a temporary run-once job")
        .WithDescription("Creates a temporary Quartz job from the provided dataset configuration, executes it once, then self-removes. The original scheduled job is unaffected.");

        // DELETE /api/jobs/{datasetId} - Remove a scheduled job
        group.MapDelete("/jobs/{datasetId}", async (
            string datasetId,
            IJobManagementService service,
            CancellationToken ct) =>
        {
            var result = await service.RemoveJobAsync(datasetId, ct);
            return result.Success ? Results.Ok(result) : Results.NotFound(result);
        })
        .WithName("RemoveJob")
        .WithSummary("Remove a scheduled job")
        .WithDescription("Removes a scheduled job by dataset ID. The job will no longer execute.");

        // POST /api/scheduler/pause - Pause all jobs
        group.MapPost("/scheduler/pause", async (IJobManagementService service, CancellationToken ct) =>
        {
            var result = await service.PauseAllJobsAsync(ct);
            return result.Success ? Results.Ok(result) : Results.BadRequest(result);
        })
        .WithName("PauseAllJobs")
        .WithSummary("Pause all jobs")
        .WithDescription("Suspends all scheduled jobs. Jobs will not execute until resumed.");

        // POST /api/scheduler/resume - Resume all jobs
        group.MapPost("/scheduler/resume", async (IJobManagementService service, CancellationToken ct) =>
        {
            var result = await service.ResumeAllJobsAsync(ct);
            return result.Success ? Results.Ok(result) : Results.BadRequest(result);
        })
        .WithName("ResumeAllJobs")
        .WithSummary("Resume all jobs")
        .WithDescription("Resumes all paused jobs. Jobs will execute according to their schedules.");

        // POST /api/scheduler/reschedule - Reschedule all jobs from config
        group.MapPost("/scheduler/reschedule", async (IJobManagementService service, CancellationToken ct) =>
        {
            var result = await service.RescheduleAllJobsAsync(ct);
            return result.Success ? Results.Ok(result) : Results.BadRequest(result);
        })
        .WithName("RescheduleAllJobs")
        .WithSummary("Reschedule all jobs")
        .WithDescription("Reloads all dataset configurations and reschedules jobs. Existing jobs are removed first.");

        // PATCH /api/datasets/{datasetId}/config - Update dataset JSON configuration
        group.MapPatch("/datasets/{datasetId}/config", async (
            string datasetId,
            [FromBody] DatasetConfigUpdateRequest request,
            IJobManagementService service,
            CancellationToken ct) =>
        {
            var result = await service.UpdateDatasetConfigAsync(
                datasetId,
                request.CronExpression,
                request.ParameterUpdates,
                request.UploadProvider,
                ct);

            if (!result.Success)
            {
                return result.Message.Contains("not found")
                    ? Results.NotFound(result)
                    : Results.BadRequest(result);
            }

            return Results.Ok(result);
        })
        .WithName("UpdateDatasetConfig")
        .WithSummary("Update dataset configuration")
        .WithDescription(
            "Surgically updates the dataset JSON file. Supports updating cronExpression, " +
            "named parameter values in source/sources[*].parameters, and upload provider. " +
            "Returns the full updated configuration. Use POST /api/jobs/{datasetId}/trigger to run the job afterwards.")
        .WithTags("Dataset Config");

        // --- Parameter Overrides (test environment) ---
        var overrideGroup = app.MapGroup("/api/parameter-overrides")
            .WithTags("Parameter Overrides");

        // GET /api/parameter-overrides - list all active overrides
        overrideGroup.MapGet("/", (IParameterOverrideService service) =>
            Results.Ok(service.GetAllOverrides()))
        .WithName("GetAllParameterOverrides")
        .WithSummary("Get all parameter overrides")
        .WithDescription("Returns all active in-memory parameter overrides. Values take precedence over the dataset JSON configuration.");

        // GET /api/parameter-overrides/{paramName} - get a single override
        overrideGroup.MapGet("/{paramName}", (string paramName, IParameterOverrideService service) =>
        {
            if (service.TryGetOverride(paramName, out var value))
                return Results.Ok(new { paramName, value });

            return Results.NotFound(new { Error = $"No override found for parameter '{paramName}'." });
        })
        .WithName("GetParameterOverride")
        .WithSummary("Get a single parameter override")
        .WithDescription("Returns the active in-memory override value for the specified parameter name. Lookup is case-insensitive.");

        // PUT /api/parameter-overrides/{paramName} - set or replace an override
        overrideGroup.MapPut("/{paramName}", (string paramName, [FromBody] ParameterOverrideRequest request, IParameterOverrideService service) =>
        {
            if (string.IsNullOrWhiteSpace(request.Value))
                return Results.BadRequest(new { Error = "Value must not be empty." });

            service.SetOverride(paramName, request.Value);
            return Results.Ok(new { paramName, value = request.Value });
        })
        .WithName("SetParameterOverride")
        .WithSummary("Set a parameter override")
        .WithDescription("Stores an in-memory override for the specified parameter name. Applies globally to all datasets on the next execution.");

        // DELETE /api/parameter-overrides/{paramName} - remove a single override
        overrideGroup.MapDelete("/{paramName}", (string paramName, IParameterOverrideService service) =>
        {
            service.RemoveOverride(paramName);
            return Results.NoContent();
        })
        .WithName("RemoveParameterOverride")
        .WithSummary("Remove a parameter override")
        .WithDescription("Removes the in-memory override for the specified parameter name. The dataset JSON configuration value will be used on next execution.");

        // DELETE /api/parameter-overrides - clear all overrides
        overrideGroup.MapDelete("/", (IParameterOverrideService service) =>
        {
            service.ClearAllOverrides();
            return Results.NoContent();
        })
        .WithName("ClearAllParameterOverrides")
        .WithSummary("Clear all parameter overrides")
        .WithDescription("Removes all active in-memory parameter overrides.");
    }
}
