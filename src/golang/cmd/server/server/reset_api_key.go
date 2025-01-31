package server

import (
	"context"
	"net/http"

	"github.com/aqueducthq/aqueduct/lib/collections/user"
	"github.com/aqueducthq/aqueduct/lib/database"
	"github.com/dropbox/godropbox/errors"
)

type resetApiKeyArgs struct {
	*CommonArgs
}

type resetApiKeyResponse struct {
	ApiKey string `json:"apiKey"`
}

type ResetApiKeyHandler struct {
	PostHandler

	Database   database.Database
	UserWriter user.Writer
}

func (*ResetApiKeyHandler) Name() string {
	return "ResetApiKey"
}

func (*ResetApiKeyHandler) Prepare(r *http.Request) (interface{}, int, error) {
	common, statusCode, err := ParseCommonArgs(r)
	if err != nil {
		return nil, statusCode, errors.Wrap(err, "Unable to reset API key.")
	}

	return &resetApiKeyArgs{
		CommonArgs: common,
	}, http.StatusOK, nil
}

func (h *ResetApiKeyHandler) Perform(ctx context.Context, interfaceArgs interface{}) (interface{}, int, error) {
	args := interfaceArgs.(*resetApiKeyArgs)
	emptyResp := resetApiKeyResponse{}

	userObject, err := h.UserWriter.ResetApiKey(ctx, args.Id, h.Database)
	if err != nil {
		return emptyResp, http.StatusInternalServerError, errors.Wrap(err, "Unable to reset API key.")
	}

	return resetApiKeyResponse{
		ApiKey: userObject.ApiKey,
	}, http.StatusOK, nil
}
