// +build integration

package elasticsearch_bridge_test

import (
	"flag"
	"testing"
	"time"

	. "github.com/cloudtrust/elasticsearch-bridge/internal/elasticsearch_bridge"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var (
	hostPort = flag.String("hostport", "elasticsearch-data:9200", "elasticsearch host:port")
)

func TestIntClient(t *testing.T) {
	var config = Config{
		Addr:    *hostPort,
		Timeout: 5 * time.Second,
	}

	var UUID string
	{
		var UUIDgen, _ = uuid.NewUUID()
		UUID = UUIDgen.String()
	}

	var client *Client
	var err error
	{
		client, err = New(config)
		assert.Nil(t, err)
	}

	{
		_, err = client.GetIndex(UUID)
		assert.NotNil(t, err)
	}

	var indexes []IndexRepresentation
	{
		indexes, err = client.ListIndexes()
		assert.Nil(t, err)
		var numberOfIndexes = len(indexes)

		_, err = client.GetIndex(UUID)
		assert.NotNil(t, err)

		err = client.CreateIndex(UUID)
		assert.Nil(t, err)

		indexes, err = client.ListIndexes()
		assert.Nil(t, err)
		var numberOfIndexesAfterCreation = len(indexes)

		assert.True(t, numberOfIndexes+1 == numberOfIndexesAfterCreation)
	}

	var index IndexSettingsRepresentation
	{
		index, err = client.GetIndex(UUID)
		assert.Nil(t, err)
		assert.NotNil(t, index)
	}

	var indexesNew []IndexRepresentation
	{
		err = client.DeleteIndex(UUID)
		assert.Nil(t, err)

		indexesNew, err = client.ListIndexes()
		assert.Nil(t, err)
		assert.Equal(t, len(indexes)-1, len(indexesNew))
	}

}

func TestIntHealth(t *testing.T) {
	var config = Config{
		Addr:    *hostPort,
		Timeout: 5 * time.Second,
	}

	var client *Client
	var err error
	{
		client, err = New(config)
		assert.Nil(t, err)
	}

	{
		var health, err = client.Health()
		assert.NotNil(t, err)
		assert.NotNil(t, health.ClusterName)
	}
}
