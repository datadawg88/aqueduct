package server

import (
	"context"
	"net/http"

	"github.com/aqueducthq/aqueduct/internal/server/utils"
	"github.com/aqueducthq/aqueduct/lib/collections/artifact"
	"github.com/aqueducthq/aqueduct/lib/collections/operator"
	"github.com/aqueducthq/aqueduct/lib/collections/shared"
	"github.com/aqueducthq/aqueduct/lib/collections/user"
	"github.com/aqueducthq/aqueduct/lib/collections/workflow"
	"github.com/aqueducthq/aqueduct/lib/collections/workflow_dag"
	"github.com/aqueducthq/aqueduct/lib/collections/workflow_dag_edge"
	"github.com/aqueducthq/aqueduct/lib/collections/workflow_dag_result"
	"github.com/aqueducthq/aqueduct/lib/database"
	workflow_utils "github.com/aqueducthq/aqueduct/lib/workflow/utils"
	"github.com/dropbox/godropbox/errors"
	"github.com/go-chi/chi"
	"github.com/google/uuid"
)

// Route: /workflow/{workflowId}
// Method: GET
// Params:
//	`workflowId`: ID for `workflow` object
// Request:
//	Headers:
//		`api-key`: user's API Key
// Response:
//	Body:
//		serialized `getWorkflowResponse`,
//		all metadata and results information for the given `workflowId`

type getWorkflowArgs struct {
	*CommonArgs
	workflowId uuid.UUID
}

type getWorkflowResponse struct {
	// a map of workflow dags keyed by their IDs
	WorkflowDags map[uuid.UUID]*workflow_dag.WorkflowDag `json:"workflow_dags"`
	// a list of dag results. Each result's `workflow_dag_id` field correspond to the
	WorkflowDagResults []workflowDagResult `json:"workflow_dag_results"`
	// a list of auth0Ids associated with workflow watchers
	WatcherAuthIds []string `json:"watcherAuthIds"`
}

type workflowDagResult struct {
	Id            uuid.UUID              `json:"id"`
	CreatedAt     int64                  `json:"created_at"`
	Status        shared.ExecutionStatus `json:"status"`
	WorkflowDagId uuid.UUID              `json:"workflow_dag_id"`
}

type GetWorkflowHandler struct {
	GetHandler

	Database                database.Database
	ArtifactReader          artifact.Reader
	OperatorReader          operator.Reader
	UserReader              user.Reader
	WorkflowReader          workflow.Reader
	WorkflowDagReader       workflow_dag.Reader
	WorkflowDagEdgeReader   workflow_dag_edge.Reader
	WorkflowDagResultReader workflow_dag_result.Reader
}

func (*GetWorkflowHandler) Name() string {
	return "GetWorkflow"
}

func (h *GetWorkflowHandler) Prepare(r *http.Request) (interface{}, int, error) {
	common, statusCode, err := ParseCommonArgs(r)
	if err != nil {
		return nil, statusCode, err
	}

	workflowIdStr := chi.URLParam(r, utils.WorkflowIdUrlParam)
	workflowId, err := uuid.Parse(workflowIdStr)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrap(err, "Malformed workflow ID.")
	}

	ok, err := h.WorkflowReader.ValidateWorkflowOwnership(
		r.Context(),
		workflowId,
		common.OrganizationId,
		h.Database,
	)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "Unexpected error during workflow ownership validation.")
	}
	if !ok {
		return nil, http.StatusBadRequest, errors.Wrap(err, "The organization does not own this workflow.")
	}

	return &getWorkflowArgs{
		CommonArgs: common,
		workflowId: workflowId,
	}, http.StatusOK, nil
}

func (h *GetWorkflowHandler) Perform(ctx context.Context, interfaceArgs interface{}) (interface{}, int, error) {
	args := interfaceArgs.(*getWorkflowArgs)

	emptyResp := getWorkflowResponse{}

	dbWorkflowDags, err := h.WorkflowDagReader.GetWorkflowDagsByWorkflowId(
		ctx,
		args.workflowId,
		h.Database,
	)
	if err != nil {
		return emptyResp, http.StatusInternalServerError, errors.Wrap(err, "Unexpected error occurred when retrieving workflow.")
	}

	workflowDags := make(map[uuid.UUID]*workflow_dag.WorkflowDag, len(dbWorkflowDags))
	for _, dbWorkflowDag := range dbWorkflowDags {
		constructedDag, err := workflow_utils.ReadWorkflowDagFromDatabase(
			ctx,
			dbWorkflowDag.Id,
			h.WorkflowReader,
			h.WorkflowDagReader,
			h.OperatorReader,
			h.ArtifactReader,
			h.WorkflowDagEdgeReader,
			h.Database,
		)
		if err != nil {
			return emptyResp, http.StatusInternalServerError, errors.Wrap(err, "Unexpected error occurred when retrieving workflow.")
		}

		workflowDags[dbWorkflowDag.Id] = constructedDag
	}

	dbWorkflowDagResults, err := h.WorkflowDagResultReader.GetWorkflowDagResultsByWorkflowId(
		ctx,
		args.workflowId,
		h.Database,
	)
	if err != nil {
		return emptyResp, http.StatusInternalServerError, errors.Wrap(err, "Unexpected error occurred when retrieving workflow.")
	}

	workflowDagResults := make([]workflowDagResult, 0, len(dbWorkflowDagResults))
	for _, dbWorkflowDagResult := range dbWorkflowDagResults {
		workflowDagResult := workflowDagResult{
			Id:            dbWorkflowDagResult.Id,
			CreatedAt:     dbWorkflowDagResult.CreatedAt.Unix(),
			Status:        dbWorkflowDagResult.Status,
			WorkflowDagId: dbWorkflowDagResult.WorkflowDagId,
		}

		workflowDagResults = append(workflowDagResults, workflowDagResult)
	}

	workflowWatcherUsers, err := h.UserReader.GetWatchersByWorkflowId(ctx, args.workflowId, h.Database)
	if err != nil {
		return emptyResp, http.StatusInternalServerError, errors.Wrap(err, "Unexpected error occurred when retrieving users watching current workflow.")
	}

	WatcherAuthIds := []string{}
	for _, user := range workflowWatcherUsers {
		WatcherAuthIds = append(WatcherAuthIds, user.Auth0Id)
	}

	return getWorkflowResponse{
		WorkflowDags:       workflowDags,
		WorkflowDagResults: workflowDagResults,
		WatcherAuthIds:     WatcherAuthIds,
	}, http.StatusOK, nil
}
