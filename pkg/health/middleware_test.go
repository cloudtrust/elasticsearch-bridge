package health_test

// //go:generate mockgen -destination=./mock/idGenerator.go -package=mock -mock_names=IDGeneratorModule=FlakiModule github.com/cloudtrust/elasticsearch-bridge/pkg/flaki IDGeneratorModule


// import (
// 	"context"
// 	"math/rand"
// 	"strconv"
// 	"testing"
// 	"time"

// 	. "github.com/cloudtrust/elasticsearch-bridge/pkg/health"
// 	"github.com/cloudtrust/elasticsearch-bridge/pkg/health/mock"
// 	"github.com/golang/mock/gomock"
// )

// func TestEndpointCorrelationIDMW(t *testing.T) {
// 	var mockCtrl = gomock.NewController(t)
// 	defer mockCtrl.Finish()
// 	var mockComponent = mock.NewHealthChecker(mockCtrl)
// 	var mockFlakiModule = mock.NewFlakiModule(mockCtrl)

// 	var m = MakeEndpointCorrelationIDMW(mockFlakiModule)(MakeExecInfluxHealthCheckEndpoint(mockComponent))

// 	rand.Seed(time.Now().UnixNano())
// 	var flakiID = strconv.FormatUint(rand.Uint64(), 10)
// 	var corrID = strconv.FormatUint(rand.Uint64(), 10)
// 	var ctx = context.WithValue(context.Background(), "correlation_id", corrID)
// 	var ctxFID = context.WithValue(context.Background(), "correlation_id", flakiID)

// 	// Context with correlation ID.
// 	mockComponent.EXPECT().ExecInfluxHealthChecks(ctx).Return(nil).Times(1)
// 	m(ctx, nil)

// 	// Without correlation ID.
// 	mockFlakiModule.EXPECT().NextValidID(gomock.Any()).Return(flakiID).Times(1)
// 	mockComponent.EXPECT().ExecInfluxHealthChecks(ctxFID).Return(nil).Times(1)
// 	m(context.Background(), nil)
// }
