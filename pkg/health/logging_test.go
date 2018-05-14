package health_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	. "github.com/cloudtrust/flaki-service/pkg/health"
	"github.com/cloudtrust/flaki-service/pkg/health/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestEndpointLoggingMW(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockLogger = mock.NewLogger(mockCtrl)
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var m = MakeEndpointLoggingMW(mockLogger)(MakeInfluxHealthCheckEndpoint(mockComponent))

	// Context with correlation ID.
	rand.Seed(time.Now().UnixNano())
	var corrID = strconv.FormatUint(rand.Uint64(), 10)
	var ctx = context.WithValue(context.Background(), "correlation_id", corrID)
	var rep = []Report{{Name: "influx", Duration: (1 * time.Second).String(), Status: OK.String()}}

	// With correlation ID.
	mockLogger.EXPECT().Log("correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
	mockComponent.EXPECT().InfluxHealthChecks(ctx).Return(rep).Times(1)
	m(ctx, nil)

	// Without correlation ID.
	mockComponent.EXPECT().InfluxHealthChecks(context.Background()).Return(rep).Times(1)
	var f = func() {
		m(context.Background(), nil)
	}
	assert.Panics(t, f)
}

func TestComponentLoggingMW(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockLogger = mock.NewLogger(mockCtrl)
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var m = MakeComponentLoggingMW(mockLogger)(mockComponent)

	rand.Seed(time.Now().UnixNano())
	var corrID = strconv.FormatUint(rand.Uint64(), 10)
	var ctx = context.WithValue(context.Background(), "correlation_id", corrID)
	var rep = func(name string) []Report {
		return []Report{{Name: name, Duration: (1 * time.Second).String(), Status: OK.String()}}
	}

	// InfluxHealthChecks.
	{
		mockComponent.EXPECT().InfluxHealthChecks(ctx).Return(rep("influx")).Times(1)
		mockLogger.EXPECT().Log("unit", "InfluxHealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
		m.InfluxHealthChecks(ctx)

		// Without correlation ID.
		mockComponent.EXPECT().InfluxHealthChecks(context.Background()).Return(rep("influx")).Times(1)
		var f = func() {
			m.InfluxHealthChecks(context.Background())
		}
		assert.Panics(t, f)
	}

	// JaegerHealthChecks.
	{
		mockComponent.EXPECT().JaegerHealthChecks(ctx).Return(rep("jaeger")).Times(1)
		mockLogger.EXPECT().Log("unit", "JaegerHealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
		m.JaegerHealthChecks(ctx)

		// Without correlation ID.
		mockComponent.EXPECT().JaegerHealthChecks(context.Background()).Return(rep("jaeger")).Times(1)
		var f = func() {
			m.JaegerHealthChecks(context.Background())
		}
		assert.Panics(t, f)
	}

	// RedisHealthChecks.
	{
		mockComponent.EXPECT().RedisHealthChecks(ctx).Return(rep("redis")).Times(1)
		mockLogger.EXPECT().Log("unit", "RedisHealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
		m.RedisHealthChecks(ctx)

		// Without correlation ID.
		mockComponent.EXPECT().RedisHealthChecks(context.Background()).Return(rep("redis")).Times(1)
		var f = func() {
			m.RedisHealthChecks(context.Background())
		}
		assert.Panics(t, f)
	}
 
	// SentryHealthChecks.
	{
		mockComponent.EXPECT().SentryHealthChecks(ctx).Return(rep("sentry")).Times(1)
		mockLogger.EXPECT().Log("unit", "SentryHealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
		m.SentryHealthChecks(ctx)

		// Without correlation ID.
		mockComponent.EXPECT().SentryHealthChecks(context.Background()).Return(rep("sentry")).Times(1)
		var f = func() {
			m.SentryHealthChecks(context.Background())
		}
		assert.Panics(t, f)
	}

	// AllHealthChecks.
	{
		var reply = map[string]string{"influx": "OK", "jaeger": "OK", "redis": "OK", "sentry": "OK"}
		mockComponent.EXPECT().AllHealthChecks(ctx).Return(reply).Times(1)
		mockLogger.EXPECT().Log("unit", "AllHealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
		m.AllHealthChecks(ctx)

		// Without correlation ID.
		mockComponent.EXPECT().AllHealthChecks(context.Background()).Return(reply).Times(1)
		var f = func() {
			m.AllHealthChecks(context.Background())
		}
		assert.Panics(t, f)
	}
}

func TestInfluxModuleLoggingMW(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockLogger = mock.NewLogger(mockCtrl)
	var mockModule = mock.NewInfluxHealthChecker(mockCtrl)

	var m = MakeInfluxModuleLoggingMW(mockLogger)(mockModule)

	// Context with correlation ID.
	rand.Seed(time.Now().UnixNano())
	var corrID = strconv.FormatUint(rand.Uint64(), 10)
	var ctx = context.WithValue(context.Background(), "correlation_id", corrID)
	var rep = []InfluxReport{{Name: "influx", Duration: (1 * time.Second), Status: OK}}

	mockModule.EXPECT().HealthChecks(ctx).Return(rep).Times(1)
	mockLogger.EXPECT().Log("unit", "HealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
	m.HealthChecks(ctx)

	// Without correlation ID.
	mockModule.EXPECT().HealthChecks(context.Background()).Return(rep).Times(1)
	var f = func() {
		m.HealthChecks(context.Background())
	}
	assert.Panics(t, f)
}

func TestJaegerModuleLoggingMW(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockLogger = mock.NewLogger(mockCtrl)
	var mockModule = mock.NewJaegerHealthChecker(mockCtrl)

	var m = MakeJaegerModuleLoggingMW(mockLogger)(mockModule)

	// Context with correlation ID.
	rand.Seed(time.Now().UnixNano())
	var corrID = strconv.FormatUint(rand.Uint64(), 10)
	var ctx = context.WithValue(context.Background(), "correlation_id", corrID)
	var rep = []JaegerReport{{Name: "jaeger", Duration: (1 * time.Second), Status: OK}}

	mockModule.EXPECT().HealthChecks(ctx).Return(rep).Times(1)
	mockLogger.EXPECT().Log("unit", "HealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
	m.HealthChecks(ctx)

	// Without correlation ID.
	mockModule.EXPECT().HealthChecks(context.Background()).Return(rep).Times(1)
	var f = func() {
		m.HealthChecks(context.Background())
	}
	assert.Panics(t, f)
}

func TestRedisModuleLoggingMW(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockLogger = mock.NewLogger(mockCtrl)
	var mockModule = mock.NewRedisHealthChecker(mockCtrl)

	var m = MakeRedisModuleLoggingMW(mockLogger)(mockModule)

	// Context with correlation ID.
	rand.Seed(time.Now().UnixNano())
	var corrID = strconv.FormatUint(rand.Uint64(), 10)
	var ctx = context.WithValue(context.Background(), "correlation_id", corrID)
	var rep = []RedisReport{{Name: "redis", Duration: (1 * time.Second), Status: OK}}

	mockModule.EXPECT().HealthChecks(ctx).Return(rep).Times(1)
	mockLogger.EXPECT().Log("unit", "HealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
	m.HealthChecks(ctx)

	// Without correlation ID.
	mockModule.EXPECT().HealthChecks(context.Background()).Return(rep).Times(1)
	var f = func() {
		m.HealthChecks(context.Background())
	}
	assert.Panics(t, f)
}

func TestSentryModuleLoggingMW(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockLogger = mock.NewLogger(mockCtrl)
	var mockModule = mock.NewSentryHealthChecker(mockCtrl)

	var m = MakeSentryModuleLoggingMW(mockLogger)(mockModule)

	// Context with correlation ID.
	rand.Seed(time.Now().UnixNano())
	var corrID = strconv.FormatUint(rand.Uint64(), 10)
	var ctx = context.WithValue(context.Background(), "correlation_id", corrID)
	var rep = []SentryReport{{Name: "sentry", Duration: (1 * time.Second), Status: OK}}

	mockModule.EXPECT().HealthChecks(ctx).Return(rep).Times(1)
	mockLogger.EXPECT().Log("unit", "HealthChecks", "correlation_id", corrID, "took", gomock.Any()).Return(nil).Times(1)
	m.HealthChecks(ctx)

	// Without correlation ID.
	mockModule.EXPECT().HealthChecks(context.Background()).Return(rep).Times(1)
	var f = func() {
		m.HealthChecks(context.Background())
	}
	assert.Panics(t, f)
}
