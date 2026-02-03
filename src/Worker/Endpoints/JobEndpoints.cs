using DataLakeIngestionService.Core.Models;
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

        // POST /api/jobs - Add and execute job from configuration
        group.MapPost("/jobs", async (
            [FromBody] DatasetConfiguration config,
            [FromQuery] bool triggerImmediately,
            IJobManagementService service,
            CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(config.DatasetId))
            {
                return Results.BadRequest(new { Error = "DatasetId is required." });
            }

            var result = await service.AddAndTriggerJobAsync(config, triggerImmediately, ct);
            return result.Success ? Results.Created($"/api/jobs/{config.DatasetId}", result) : Results.BadRequest(result);
        })
        .WithName("AddJob")
        .WithSummary("Add and optionally trigger a new job")
        .WithDescription("Creates a new scheduled job from the provided dataset configuration. Set triggerImmediately=true to execute it right away.");

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
    }
}
