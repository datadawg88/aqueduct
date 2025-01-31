package authentication

import (
	"context"
	"net/http"

	"github.com/aqueducthq/aqueduct/internal/server/utils"
	"github.com/aqueducthq/aqueduct/lib/collections/user"
	"github.com/aqueducthq/aqueduct/lib/database"
)

//	The `RequireApiKey` middleware expects a request whose header contains
//	key `api-key` for authorization purposes. If the authorization is successful,
//	it forwards the request to the controller. Otherwise, it sends an http response
//	in JSON format with an `error` message.
func RequireApiKey(userReader user.Reader, db database.Database) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := r.Header.Get(utils.ApiKeyHeader)

			userObject, err := userReader.GetUserFromApiKey(r.Context(), apiKey, db)
			if err == database.ErrNoRows {
				utils.SendErrorResponse(w, "Invalid API key credentials.", http.StatusForbidden)
			} else if err != nil {
				// Something went wrong with accessing the database
				utils.SendErrorResponse(w, "Unable to validate API key credentials.", http.StatusForbidden)
			} else {
				// Create a new context with userId and organizationId.
				contextWithUserId := context.WithValue(r.Context(), utils.UserIdKey, userObject.Id.String())
				contextWithOrganizationId := context.WithValue(contextWithUserId, utils.OrganizationIdKey, userObject.OrganizationId)
				contextWithUserAuth0Id := context.WithValue(contextWithOrganizationId, utils.UserAuth0IdKey, userObject.Auth0Id)
				h.ServeHTTP(w, r.WithContext(contextWithUserAuth0Id))
			}
		})
	}
}
