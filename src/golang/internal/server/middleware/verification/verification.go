package verification

import (
	"net/http"

	"github.com/aqueducthq/aqueduct/internal/server/utils"
)

// Verification middleware for requests coming to server.
// We currently only perform additional verifications on requests coming from the SDK.
// All other requests are automatically allowed to pass and are treated as no-op
func VerifyRequest() func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientVersionFromHeader := r.Header.Get(utils.SdkClientVersionHeader)
			// We current use the utils.SdkClientVersionHeader to determine if that request is coming from the sdk.
			// In the event that it is, ensure the proper validations are done.
			if clientVersionFromHeader != "" {
				httpResponse, reason := VerifySdkRequest(clientVersionFromHeader)
				if httpResponse != http.StatusOK {
					utils.SendErrorResponse(w, reason, httpResponse)
					return
				}
			}

			h.ServeHTTP(w, r)
		})
	}
}
