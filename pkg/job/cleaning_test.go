//go:generate mockgen -source=cleaning.go -destination=./mock/cleaning.go -package=mock -mock_names=ElasticsearchClient=ElasticsearchClient github.com/cloudtrust/elasticsearch-bridge/pkg/job ElasticsearchClient

package job_test

import (

	"testing"
	"time"
	"context"

	"github.com/stretchr/testify/assert"
	"github.com/golang/mock/gomock"
	. "github.com/cloudtrust/elasticsearch-bridge/pkg/job"
	client "github.com/cloudtrust/elasticsearch-bridge/internal/elasticsearch_bridge"
	mock "github.com/cloudtrust/elasticsearch-bridge/pkg/job/mock"
)


func TestIntMakeElasticsearchCleanIndexJob(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockClient = mock.NewElasticsearchClient(mockCtrl)

	var indexWrongFormat = client.IndexRepresentation{
		Index: "wrong-format",
	}
	var indexOld = client.IndexRepresentation{
		Index: "int-elastic-1800.10.02",
	}
	var indexNew = client.IndexRepresentation{
		Index: "int-elastic-2018.10.01",
	}
	var allIndexes = []client.IndexRepresentation{indexWrongFormat, indexOld, indexNew}
	mockClient.EXPECT().ListIndexes().Return(allIndexes, nil).Times(1)
	mockClient.EXPECT().DeleteIndex("int-elastic-1800.10.02").Return(nil).Times(1)

	var job, err = MakeElasticsearchCleanIndexJob(mockClient, 24*time.Hour * 365*100)
	var steps = job.Steps()

	assert.Nil(t, err)
	assert.Equal(t, 3, len(steps))

	var res1, err1 = job.Steps()[0](context.Background(), nil)
	assert.Nil(t, err1)

	var indexes = res1.([]string)
	assert.Equal(t, []string{"wrong-format", "int-elastic-1800.10.02", "int-elastic-2018.10.01"}, indexes)

	var res2, err2 = job.Steps()[1](context.Background(), res1)
	assert.Nil(t, err2)

	var filteredIndexes = res2.([]string)
	assert.Equal(t, []string{"int-elastic-1800.10.02"}, filteredIndexes)

	var res3, err3 = job.Steps()[2](context.Background(), res2)
	assert.Nil(t, err3)

	var result = res3.(string)
	assert.Equal(t,"ok", result)
}
