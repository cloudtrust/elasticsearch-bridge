package health_test

//go:generate mockgen -destination=./mock/elasticsearch.go -package=mock -mock_names=ElasticsearchClient=ElasticsearchClient  github.com/cloudtrust/elasticsearch-bridge/pkg/health ElasticsearchClient
//go:generate mockgen -destination=./mock/logging.go -package=mock -mock_names=Logger=Logger github.com/go-kit/kit/log Logger

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	// "math/rand"
	// "strconv"
	"encoding/json"
	"testing"
	"time"

	internal "github.com/cloudtrust/elasticsearch-bridge/internal/elasticsearch_bridge"
	. "github.com/cloudtrust/elasticsearch-bridge/pkg/health"
	mock "github.com/cloudtrust/elasticsearch-bridge/pkg/health/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestElasticsearchHealthChecks(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockElasticsearchClient = mock.NewElasticsearchClient(mockCtrl)

	var m = NewElasticsearchModule(mockElasticsearchClient)

	var indexWrongFormat = internal.IndexRepresentation{
		Index: "wrong-format",
	}
	var indexOld = internal.IndexRepresentation{
		Index: "int-elastic-1800.10.02",
	}
	var indexNew = internal.IndexRepresentation{
		Index: "int-elastic-2018.10.01",
	}

	var allIndexes = []internal.IndexRepresentation{indexWrongFormat, indexOld, indexNew}
	mockElasticsearchClient.EXPECT().ListIndexes().Return(allIndexes, nil).Times(1)
	mockElasticsearchClient.EXPECT().CreateIndex(gomock.Any()).Return(nil).Times(1)
	mockElasticsearchClient.EXPECT().DeleteIndex(gomock.Any()).Return(nil).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "green"}, nil).Times(1)

	var reports = m.HealthChecks(context.Background())
	assert.Equal(t, 2, len(reports))
	assert.Equal(t, "Health", reports[0].Name)
	assert.NotZero(t, reports[0].Duration)
	assert.Equal(t, OK, reports[0].Status)
	assert.Zero(t, reports[0].Error)
	assert.Equal(t, "Index API", reports[1].Name)
	assert.NotZero(t, reports[1].Duration)
	assert.Equal(t, OK, reports[1].Status)
	assert.Zero(t, reports[1].Error)
}

func TestElasticsearchHealthChecksFailureErrorDeletion(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockElasticsearchClient = mock.NewElasticsearchClient(mockCtrl)

	var m = NewElasticsearchModule(mockElasticsearchClient)

	var indexWrongFormat = internal.IndexRepresentation{
		Index: "wrong-format",
	}
	var indexOld = internal.IndexRepresentation{
		Index: "int-elastic-1800.10.02",
	}
	var indexNew = internal.IndexRepresentation{
		Index: "int-elastic-2018.10.01",
	}

	var allIndexes = []internal.IndexRepresentation{indexWrongFormat, indexOld, indexNew}
	mockElasticsearchClient.EXPECT().ListIndexes().Return(allIndexes, nil).Times(1)
	mockElasticsearchClient.EXPECT().CreateIndex(gomock.Any()).Return(nil).Times(1)
	mockElasticsearchClient.EXPECT().DeleteIndex(gomock.Any()).Return(fmt.Errorf("Fail to delete")).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "green"}, nil).Times(1)

	var reports = m.HealthChecks(context.Background())
	assert.Equal(t, 2, len(reports))
	assert.Equal(t, "Health", reports[0].Name)
	assert.NotZero(t, reports[0].Duration)
	assert.Equal(t, OK, reports[0].Status)
	assert.Zero(t, reports[0].Error)
	assert.Equal(t, "Index API", reports[1].Name)
	assert.NotZero(t, reports[1].Duration)
	assert.Equal(t, KO, reports[1].Status)
	assert.NotZero(t, reports[1].Error)
}

func TestElasticsearchHealthChecksFailureErrorCreation(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockElasticsearchClient = mock.NewElasticsearchClient(mockCtrl)

	var m = NewElasticsearchModule(mockElasticsearchClient)

	var indexWrongFormat = internal.IndexRepresentation{
		Index: "wrong-format",
	}
	var indexOld = internal.IndexRepresentation{
		Index: "int-elastic-1800.10.02",
	}
	var indexNew = internal.IndexRepresentation{
		Index: "int-elastic-2018.10.01",
	}

	var allIndexes = []internal.IndexRepresentation{indexWrongFormat, indexOld, indexNew}
	mockElasticsearchClient.EXPECT().ListIndexes().Return(allIndexes, nil).Times(1)
	mockElasticsearchClient.EXPECT().CreateIndex(gomock.Any()).Return(fmt.Errorf("Fail to create")).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "green"}, nil).Times(1)

	var reports = m.HealthChecks(context.Background())
	assert.Equal(t, 2, len(reports))
	assert.Equal(t, "Health", reports[0].Name)
	assert.NotZero(t, reports[0].Duration)
	assert.Equal(t, OK, reports[0].Status)
	assert.Zero(t, reports[0].Error)
	assert.Equal(t, "Index API", reports[1].Name)
	assert.NotZero(t, reports[1].Duration)
	assert.Equal(t, KO, reports[1].Status)
	assert.NotZero(t, reports[1].Error)
}

func TestElasticsearchHealthChecksFailureErrorListing(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockElasticsearchClient = mock.NewElasticsearchClient(mockCtrl)

	var m = NewElasticsearchModule(mockElasticsearchClient)

	mockElasticsearchClient.EXPECT().ListIndexes().Return(nil, fmt.Errorf("Fail to list")).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "green"}, nil).Times(1)

	var reports = m.HealthChecks(context.Background())
	assert.Equal(t, 2, len(reports))
	assert.Equal(t, "Health", reports[0].Name)
	assert.NotZero(t, reports[0].Duration)
	assert.Equal(t, OK, reports[0].Status)
	assert.Zero(t, reports[0].Error)
	assert.Equal(t, "Index API", reports[1].Name)
	assert.NotZero(t, reports[1].Duration)
	assert.Equal(t, KO, reports[1].Status)
	assert.NotZero(t, reports[1].Error)
}

func TestElasticsearchHealthChecksFailure(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockElasticsearchClient = mock.NewElasticsearchClient(mockCtrl)

	var m = NewElasticsearchModule(mockElasticsearchClient)

	mockElasticsearchClient.EXPECT().ListIndexes().Return(nil, fmt.Errorf("Fail to list")).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "yellow"}, nil).Times(1)

	var reports = m.HealthChecks(context.Background())
	assert.Equal(t, "Health", reports[0].Name)
	assert.NotZero(t, reports[0].Duration)
	assert.Equal(t, Degraded, reports[0].Status)
	assert.Zero(t, reports[0].Error)

	mockElasticsearchClient.EXPECT().ListIndexes().Return(nil, fmt.Errorf("Fail to list")).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "red"}, nil).Times(1)

	reports = m.HealthChecks(context.Background())
	assert.Equal(t, "Health", reports[0].Name)
	assert.NotZero(t, reports[0].Duration)
	assert.Equal(t, KO, reports[0].Status)
	assert.Zero(t, reports[0].Error)

	mockElasticsearchClient.EXPECT().ListIndexes().Return(nil, fmt.Errorf("Fail to list")).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "nope"}, nil).Times(1)

	reports = m.HealthChecks(context.Background())
	assert.Equal(t, "Health", reports[0].Name)
	assert.NotZero(t, reports[0].Duration)
	assert.Equal(t, Unknown, reports[0].Status)
	assert.Zero(t, reports[0].Error)

	mockElasticsearchClient.EXPECT().ListIndexes().Return(nil, fmt.Errorf("Fail to list")).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{}, fmt.Errorf("Fail to check health")).Times(1)

	reports = m.HealthChecks(context.Background())
	assert.Equal(t, "Health", reports[0].Name)
	assert.NotZero(t, reports[0].Duration)
	assert.Equal(t, KO, reports[0].Status)
	assert.NotZero(t, reports[0].Error)


}

func TestElasticsearchModuleLoggingMW(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockElasticsearchClient = mock.NewElasticsearchClient(mockCtrl)
	var mockLogger = mock.NewLogger(mockCtrl)

	var module = NewElasticsearchModule(mockElasticsearchClient)
	var m = MakeElasticsearchModuleLoggingMW(mockLogger)(module)

	// Context with correlation ID.
	rand.Seed(time.Now().UnixNano())
	var corrID = strconv.FormatUint(rand.Uint64(), 10)
	var ctx = context.WithValue(context.Background(), "correlation_id", corrID)

	mockElasticsearchClient.EXPECT().ListIndexes().Return([]internal.IndexRepresentation{}, nil).Times(1)
	mockElasticsearchClient.EXPECT().CreateIndex(gomock.Any()).Return(nil).Times(1)
	mockElasticsearchClient.EXPECT().DeleteIndex(gomock.Any()).Return(nil).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "green"}, nil).Times(1)
	mockLogger.EXPECT().Log("unit", "HealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
	m.HealthChecks(ctx)

	mockElasticsearchClient.EXPECT().ListIndexes().Return([]internal.IndexRepresentation{}, nil).Times(1)
	mockElasticsearchClient.EXPECT().CreateIndex(gomock.Any()).Return(nil).Times(1)
	mockElasticsearchClient.EXPECT().DeleteIndex(gomock.Any()).Return(nil).Times(1)
	mockElasticsearchClient.EXPECT().Health().Return(internal.HealthRepresentation{Status: "green"}, nil).Times(1)
	// Without correlation ID.
	var f = func() {
		m.HealthChecks(context.Background())
	}
	assert.Panics(t, f)
}

func TestElasticsearchReportMarshalJSON(t *testing.T) {
	var report = &ElasticsearchReport{
		Name:     "Elastic",
		Duration: 1 * time.Second,
		Status:   OK,
		Error:    fmt.Errorf("Error"),
	}

	jsonR, err := report.MarshalJSON()

	assert.Nil(t, err)
	assert.Equal(t, "{\"name\":\"Elastic\",\"duration\":\"1s\",\"status\":\"OK\",\"error\":\"Error\"}", string(jsonR))

	var reportBis = &ElasticsearchReport{
		Name:     "Elastic",
		Duration: 1 * time.Second,
		Status:   OK,
		Error:    fmt.Errorf("Error"),
		Infos:    json.RawMessage(`{"status": "green"}`),
	}

	jsonBis, err := reportBis.MarshalJSON()

	assert.Nil(t, err)
	assert.Equal(t, "{\"name\":\"Elastic\",\"duration\":\"1s\",\"status\":\"OK\",\"error\":\"Error\",\"infos\":{\"status\":\"green\"}}", string(jsonBis))
}
