package operator

import (
	"context"

	"github.com/aqueducthq/aqueduct/lib/database"
	"github.com/google/uuid"
)

type Operator struct {
	Id          uuid.UUID `db:"id" json:"id"`
	Name        string    `db:"name" json:"name"`
	Description string    `db:"description" json:"description"`
	Spec        Spec      `db:"spec" json:"spec"`

	/* Fields not stored in DB */
	Inputs  []uuid.UUID `json:"inputs"`
	Outputs []uuid.UUID `json:"outputs"`
}

type Reader interface {
	Exists(ctx context.Context, id uuid.UUID, db database.Database) (bool, error)
	GetOperator(ctx context.Context, id uuid.UUID, db database.Database) (*Operator, error)
	GetOperators(ctx context.Context, ids []uuid.UUID, db database.Database) ([]Operator, error)
	GetOperatorsByWorkflowDagId(
		ctx context.Context,
		workflowDagId uuid.UUID,
		db database.Database,
	) ([]Operator, error)
	ValidateOperatorOwnership(
		ctx context.Context,
		organizationId string,
		operatorId uuid.UUID,
		db database.Database,
	) (bool, error)
}

type Writer interface {
	CreateOperator(
		ctx context.Context,
		name string,
		description string,
		spec *Spec,
		db database.Database,
	) (*Operator, error)
	UpdateOperator(
		ctx context.Context,
		id uuid.UUID,
		changes map[string]interface{},
		db database.Database,
	) (*Operator, error)
	DeleteOperator(ctx context.Context, id uuid.UUID, db database.Database) error
	DeleteOperators(ctx context.Context, ids []uuid.UUID, db database.Database) error
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
