package registry

import (
	"fmt"
	"reflect"

	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/deployer"
	"go.flow.arcalot.io/pluginsdk/schema"
)

// Registry describes the functions a deployer registry must implement.
type Registry interface {
	// List lists the registered deployers with their scopes.
	List() map[string]schema.Object
	// DeploymentTypes returns a slice of all deployment types in the registry.
	DeploymentTypes() []deployer.DeploymentType
	// DeployConfigSchema returns a composite schema for all types in the registry of that deployment type.
	DeployConfigSchema(deploymentType deployer.DeploymentType) schema.OneOf[string]
	// Create creates a connector with the given configuration type. The registry must identify the correct deployer
	// based on the type passed.
	Create(deploymentType deployer.DeploymentType, config any, logger log.Logger) (deployer.Connector, error)
}

type registry struct {
	deployerFactories map[deployer.DeploymentType]map[string]deployer.AnyConnectorFactory
}

func (r registry) DeploymentTypes() []deployer.DeploymentType {
	typeList := make([]deployer.DeploymentType, len(r.deployerFactories))

	i := 0
	for k := range r.deployerFactories {
		typeList[i] = k
		i++
	}
	return typeList
}

func (r registry) List() map[string]schema.Object {
	result := make(map[string]schema.Object, len(r.deployerFactories))
	for _, factories := range r.deployerFactories {
		for id, factory := range factories {
			result[id] = factory.ConfigurationSchema()
		}
	}
	return result
}

func (r registry) DeployConfigSchema(deploymentType deployer.DeploymentType) schema.OneOf[string] {
	schemas := make(map[string]schema.Object, len(r.deployerFactories))
	for id, factory := range r.deployerFactories[deploymentType] {
		schemas[id] = factory.ConfigurationSchema()
	}
	return schema.NewOneOfStringSchema[any](
		schemas,
		"deployer_id",
	)
}

func (r registry) Create(deploymentType deployer.DeploymentType, config any, logger log.Logger) (deployer.Connector, error) {
	if config == nil {
		return nil, fmt.Errorf("the deployer configuration cannot be nil")
	}
	reflectedConfig := reflect.ValueOf(config)
	for _, factory := range r.deployerFactories[deploymentType] {
		if factory.ConfigurationSchema().ReflectedType() == reflectedConfig.Type() {
			return factory.Create(config, logger)
		}
	}
	return nil, fmt.Errorf("could not identify correct deployer factory for %T", config)
}
