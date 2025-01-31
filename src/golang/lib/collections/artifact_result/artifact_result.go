package artifact_result

import (
	"context"

	"github.com/aqueducthq/aqueduct/lib/collections/shared"
	"github.com/aqueducthq/aqueduct/lib/database"
	"github.com/google/uuid"
)

type ArtifactResult struct {
	Id                  uuid.UUID              `db:"id" json:"id"`
	WorkflowDagResultId uuid.UUID              `db:"workflow_dag_result_id" json:"workflow_dag_result_id"`
	ArtifactId          uuid.UUID              `db:"artifact_id" json:"artifact_id"`
	ContentPath         string                 `db:"content_path" json:"content_path"`
	Status              shared.ExecutionStatus `db:"status" json:"status"`
	Metadata            NullMetadata           `db:"metadata" json:"metadata"`
}

type Reader interface {
	GetArtifactResult(ctx context.Context, id uuid.UUID, db database.Database) (*ArtifactResult, error)
	GetArtifactResults(ctx context.Context, ids []uuid.UUID, db database.Database) ([]ArtifactResult, error)
	GetArtifactResultByWorkflowDagResultIdAndArtifactId(
		ctx context.Context,
		workflowDagResultId, artifactId uuid.UUID,
		db database.Database,
	) (*ArtifactResult, error)
	GetArtifactResultsByWorkflowDagResultIds(
		ctx context.Context,
		workflowDagResultIds []uuid.UUID,
		db database.Database,
	) ([]ArtifactResult, error)
}

type Writer interface {
	CreateArtifactResult(
		ctx context.Context,
		workflowDagResultId uuid.UUID,
		artifactId uuid.UUID,
		contentPath string,
		db database.Database,
	) (*ArtifactResult, error)
	UpdateArtifactResult(
		ctx context.Context,
		id uuid.UUID,
		changes map[string]interface{},
		db database.Database,
	) (*ArtifactResult, error)
	DeleteArtifactResult(ctx context.Context, id uuid.UUID, db database.Database) error
	DeleteArtifactResults(ctx context.Context, ids []uuid.UUID, db database.Database) error
}

func NewReader(dbConf *database.DatabaseConfig) (Reader, error) {
	if dbConf.Type == database.PostgresType {
		return newPostgresReader(), nil
	}

	if dbConf.Type == database.SqliteType {
		return newSqliteReader(), nil
	}

	return nil, database.ErrUnsupportedDbType
}

func NewWriter(dbConf *database.DatabaseConfig) (Writer, error) {
	if dbConf.Type == database.PostgresType {
		return newPostgresWriter(), nil
	}

	if dbConf.Type == database.SqliteType {
		return newSqliteWriter(), nil
	}

	return nil, database.ErrUnsupportedDbType
}
