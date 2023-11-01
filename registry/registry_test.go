package registry_test

import (
	"testing"

	"go.arcalot.io/assert"
	log "go.arcalot.io/log/v2"
	"go.flow.arcalot.io/deployer/registry"
)

func TestRegistry_Schema(t *testing.T) {
	t.Parallel()

	t.Run("correct-input", testRegistrySchemaCorrectInput)
	t.Run("incorrect-input", testRegistrySchemaIncorrectInput)
}

func testRegistrySchemaIncorrectInput(t *testing.T) {
	r := registry.New(
		&testNewFactory{},
	)
	schema := r.DeployConfigSchema("test-type")

	if _, err := schema.Unserialize(map[string]any{
		"deployer_name": "non-existent",
	}); err == nil {
		t.Fatalf("No error returned")
	}

	if _, err := schema.Unserialize(map[string]any{}); err == nil {
		t.Fatalf("No error returned")
	}
}

func testRegistrySchemaCorrectInput(t *testing.T) {
	r := registry.New(
		&testNewFactory{},
	)
	schema := r.DeployConfigSchema("test-type")

	unserializedData, err := schema.Unserialize(map[string]any{
		"deployer_name": "test",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, ok := unserializedData.(testConfig); !ok {
		t.Fatalf("Incorrect unserialized data type returned: %T", unserializedData)
	}
}

func TestRegistry_Create(t *testing.T) {
	t.Parallel()
	t.Run("correct-creation", testRegistryCreateCorrectCreation)
	t.Run("incorrect-config-type", testRegistryCreateIncorrectConfigType)
	t.Run("nil-config", testRegistryCreateNilConfig)
}

func testRegistryCreateCorrectCreation(t *testing.T) {
	t.Parallel()
	r := registry.New(
		&testNewFactory{},
	)

	connector, err := r.Create(testDeploymentType, testConfig{}, log.NewTestLogger(t))
	assert.NoError(t, err)
	if _, ok := connector.(*testConnector); !ok {
		t.Fatalf("Incorrect connector returned: %T", connector)
	}
}

func testRegistryCreateIncorrectConfigType(t *testing.T) {
	t.Parallel()

	r := registry.New(
		&testNewFactory{},
	)
	_, err := r.Create(testDeploymentType, map[string]any{}, log.NewTestLogger(t))
	if err == nil {
		t.Fatalf("expected error, no error returned")
	}
}

func testRegistryCreateNilConfig(t *testing.T) {
	t.Parallel()
	r := registry.New(
		&testNewFactory{},
	)
	_, err := r.Create(testDeploymentType, nil, log.NewTestLogger(t))
	if err == nil {
		t.Fatalf("expected error, no error returned")
	}
}
