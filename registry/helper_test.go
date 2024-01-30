package registry_test

import (
	"context"
	"fmt"

	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/pluginsdk/schema"
)

type testConfig struct {
}

type testNewFactory struct {
}

var testDeploymentType = deployer.DeploymentType("test-type")

func (t testNewFactory) Name() string {
	return "test"
}

func (t testNewFactory) DeploymentType() deployer.DeploymentType {
	return "test-type"
}

func (t testNewFactory) ConfigurationSchema() schema.Object {
	return schema.NewTypedScopeSchema[testConfig](
		schema.NewStructMappedObjectSchema[testConfig](
			"test",
			map[string]*schema.PropertySchema{},
		),
	)
}

func (t testNewFactory) Create(_ any, _ log.Logger) (deployer.Connector, error) {
	return &testConnector{}, nil
}

type testConnector struct {
}

func (t testConnector) Deploy(_ context.Context, _ string) (deployer.Plugin, error) {
	return nil, fmt.Errorf("not implemented")
}
