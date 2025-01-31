package request_parser

import (
	"encoding/json"
	"net/http"

	"github.com/aqueducthq/aqueduct/internal/server/utils"
	"github.com/aqueducthq/aqueduct/lib/collections/integration"
	"github.com/dropbox/godropbox/errors"
)

// ParseIntegrationConfigFromRequest parses the integration service, integration name, configuration,
// and whether it is a user only integration from the request
func ParseIntegrationConfigFromRequest(r *http.Request) (integration.Service, string, map[string]string, bool, error) {
	serviceStr := r.Header.Get(utils.IntegrationServiceHeader)
	service, err := integration.ParseService(serviceStr)
	if err != nil {
		return "", "", nil, false, err
	}

	configHeader := r.Header.Get(utils.IntegrationConfigHeader)
	var configuration map[string]string
	err = json.Unmarshal([]byte(configHeader), &configuration)
	if err != nil {
		return "", "", nil, false, errors.Newf("Unable to parse integration configuration: %v", err)
	}

	integrationName := r.Header.Get(utils.IntegrationNameHeader)
	if integrationName == "" {
		return "", "", nil, false, errors.New("Integration name was not provided.")
	}

	userOnly := isUserOnlyIntegration(service)

	return service, integrationName, configuration, userOnly, nil
}

// isUserOnlyIntegration returns whether the specified service is only accessible by the user.
func isUserOnlyIntegration(svc integration.Service) bool {
	userSpecific := []integration.Service{integration.GoogleSheets, integration.Github}
	for _, s := range userSpecific {
		if s == svc {
			return true
		}
	}
	return false
}
