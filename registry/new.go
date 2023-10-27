package registry

import (
	"fmt"

	"go.flow.arcalot.io/deployer"
)

// New creates a new registry with the given factories.
func New(factory ...deployer.AnyConnectorFactory) Registry {
	factories := make(map[deployer.DeploymentType]map[string]deployer.AnyConnectorFactory, len(factory))

	for _, f := range factory {
		deploymentType := f.DeploymentType()
		category, categoryCreated := factories[deploymentType]
		if !categoryCreated {
			category = make(map[string]deployer.AnyConnectorFactory)
			factories[deploymentType] = category
		}

		if v, ok := category[f.ID()]; ok {
			panic(fmt.Errorf("duplicate deployer factory ID for deployment type %s: %s (first: %T, second: %T)",
				deploymentType, f.ID(), v, f))
		}
		category[f.ID()] = f
	}

	return &registry{
		factories,
	}
}
