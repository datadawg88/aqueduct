package server

import (
	"context"
	"time"

	"github.com/dropbox/godropbox/errors"
	"github.com/google/uuid"
	"github.com/gorhill/cronexpr"
	log "github.com/sirupsen/logrus"
)

func (s *AqServer) triggerMissedCronJobs(
	ctx context.Context,
	workflowId uuid.UUID,
	cronSchedule string,
	referenceTime time.Time,
) {
	nextTwoTriggerTimeStamp := cronexpr.MustParse(cronSchedule).NextN(time.Now(), 2)
	// We subtract the next two trigger timestamp to get the duration between consecutive cron jobs.
	duration := nextTwoTriggerTimeStamp[1].Unix() - nextTwoTriggerTimeStamp[0].Unix()
	// Subtracting the duration from the next trigger timestamp gives us the last expected trigger timestamp.
	lastExpectedTriggerTime := nextTwoTriggerTimeStamp[0].Unix() - duration
	if lastExpectedTriggerTime > referenceTime.Unix() {
		// This means that the workflow should have been triggered, but it wasn't.
		// So we manually trigger the workflow here.
		_, _, err := (&RefreshWorkflowHandler{
			Database:       s.Database,
			JobManager:     s.JobManager,
			GithubManager:  s.GithubManager,
			Vault:          s.Vault,
			WorkflowReader: s.WorkflowReader,
		}).Perform(
			ctx,
			&refreshWorkflowArgs{
				workflowId: workflowId,
			},
		)
		if err != nil {
			log.Errorf("Unable to trigger workflow: %v", err)
		}
	}
}

// RunMissedCronJobs first gets the latest workflow run timestamp of all deployed workflows that are
// on a schedule and are not paused. For each workflow, it compares the latest workflow run's timestamp with the
// expected trigger timestamp calculated based on the cron schedule, and manually triggers the workflow
// if the cron triggering did not happen.
func (s *AqServer) RunMissedCronJobs() error {
	ctx := context.Background()
	workflowLastRunResponse, err := s.CustomReader.GetWorkflowLastRun(ctx, s.Database)
	if err != nil {
		return errors.Wrap(err, "Unable to get workflow last run data from database.")
	}

	workflowsRan := map[uuid.UUID]bool{}

	for _, workflowLastRun := range workflowLastRunResponse {
		if workflowLastRun.Schedule.CronSchedule != "" && !workflowLastRun.Schedule.Paused {
			s.triggerMissedCronJobs(
				ctx,
				workflowLastRun.WorkflowId,
				string(workflowLastRun.Schedule.CronSchedule),
				workflowLastRun.LastRunAt,
			)
		}
		workflowsRan[workflowLastRun.WorkflowId] = true
	}

	allWorkflows, err := s.WorkflowReader.GetAllWorkflows(ctx, s.Database)
	if err != nil {
		return errors.Wrap(err, "Unable to get workflows from database.")
	}

	for _, workflow := range allWorkflows {
		if _, ok := workflowsRan[workflow.Id]; !ok {
			// If we reach here, it means this workflow hasn't produced any run yet.
			if workflow.Schedule.CronSchedule != "" && !workflow.Schedule.Paused {
				s.triggerMissedCronJobs(
					ctx,
					workflow.Id,
					string(workflow.Schedule.CronSchedule),
					workflow.CreatedAt,
				)
			}
		}
	}

	return nil
}

func (s *AqServer) initializeWorkflowCronJobs(ctx context.Context) error {
	workflows, err := s.Readers.WorkflowReader.GetAllWorkflows(ctx, s.Database)
	if err != nil {
		return err
	}

	for _, wf := range workflows {
		if wf.Schedule.CronSchedule != "" {
			if wf.Schedule.Paused {
				wf.Schedule.CronSchedule = ""
			}

			err = createWorkflowCronJob(
				ctx,
				&wf,
				s.Database.Config(),
				s.Vault,
				s.JobManager,
				s.GithubManager,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
