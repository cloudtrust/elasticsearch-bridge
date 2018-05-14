package health_test

//go:generate mockgen -destination=./mock/component.go -package=mock -mock_names=InfluxHealthChecker=InfluxHealthChecker,JaegerHealthChecker=JaegerHealthChecker,RedisHealthChecker=RedisHealthChecker,SentryHealthChecker=SentryHealthChecker  github.com/cloudtrust/elasticsearch-bridge/pkg/health InfluxHealthChecker,JaegerHealthChecker,RedisHealthChecker,SentryHealthChecker


import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/cloudtrust/elasticsearch-bridge/pkg/health"
	"github.com/cloudtrust/elasticsearch-bridge/pkg/health/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
) 

func TestHealthChecks(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockInfluxModule = mock.NewInfluxHealthChecker(mockCtrl) 
	var mockJaegerModule = mock.NewJaegerHealthChecker(mockCtrl)
	var mockRedisModule = mock.NewRedisHealthChecker(mockCtrl)
	var mockSentryModule = mock.NewSentryHealthChecker(mockCtrl)

	mockInfluxModule.EXPECT().HealthChecks(context.Background()).Return([]InfluxReport{{Name: "influx", Duration: time.Duration(1 * time.Second), Status: OK}}).Times(2)
	mockJaegerModule.EXPECT().HealthChecks(context.Background()).Return([]JaegerReport{{Name: "jaeger", Duration: time.Duration(1 * time.Second), Status: OK}}).Times(2)
	mockRedisModule.EXPECT().HealthChecks(context.Background()).Return([]RedisReport{{Name: "redis", Duration: time.Duration(1 * time.Second), Status: OK}}).Times(2)
	mockSentryModule.EXPECT().HealthChecks(context.Background()).Return([]SentryReport{{Name: "sentry", Duration: time.Duration(1 * time.Second), Status: OK}}).Times(2)

	var c = NewComponent(mockInfluxModule, mockJaegerModule, mockRedisModule, mockSentryModule)

	// Influx.
	{
		var report = c.InfluxHealthChecks(context.Background())[0]
		assert.Equal(t, "influx", report.Name)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, "OK", report.Status)
		assert.Zero(t, report.Error)
	}
 
	// Jaeger.
	{
		var report = c.JaegerHealthChecks(context.Background())[0]
		assert.Equal(t, "jaeger", report.Name)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, "OK", report.Status)
		assert.Zero(t, report.Error)
	}

	// Redis.
	{
		var report = c.RedisHealthChecks(context.Background())[0]
		assert.Equal(t, "redis", report.Name)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, "OK", report.Status)
		assert.Zero(t, report.Error)
	}

	// Sentry.
	{
		var report = c.SentryHealthChecks(context.Background())[0]
		assert.Equal(t, "sentry", report.Name)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, "OK", report.Status)
		assert.Zero(t, report.Error)
	}

	// All.
	{
		var reply = c.AllHealthChecks(context.Background())
		assert.Equal(t, "OK", reply["influx"])
		assert.Equal(t, "OK", reply["jaeger"])
		assert.Equal(t, "OK", reply["redis"])
		assert.Equal(t, "OK", reply["sentry"])
	}
}

func TestHealthChecksFail(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockInfluxModule = mock.NewInfluxHealthChecker(mockCtrl)
	var mockJaegerModule = mock.NewJaegerHealthChecker(mockCtrl)
	var mockRedisModule = mock.NewRedisHealthChecker(mockCtrl)
	var mockSentryModule = mock.NewSentryHealthChecker(mockCtrl)

	mockInfluxModule.EXPECT().HealthChecks(context.Background()).Return([]InfluxReport{{Name: "influx", Duration: time.Duration(1 * time.Second), Status: Deactivated}}).Times(2)
	mockJaegerModule.EXPECT().HealthChecks(context.Background()).Return([]JaegerReport{{Name: "jaeger", Duration: time.Duration(1 * time.Second), Status: KO, Error: fmt.Errorf("fail")}}).Times(2)
	mockRedisModule.EXPECT().HealthChecks(context.Background()).Return([]RedisReport{{Name: "redis", Duration: time.Duration(1 * time.Second), Status: Degraded, Error: fmt.Errorf("fail")}}).Times(2)
	mockSentryModule.EXPECT().HealthChecks(context.Background()).Return([]SentryReport{{Name: "sentry", Duration: time.Duration(1 * time.Second), Status: KO, Error: fmt.Errorf("fail")}}).Times(2)

	var c = NewComponent(mockInfluxModule, mockJaegerModule, mockRedisModule, mockSentryModule)

	// Influx.
	{
		var report = c.InfluxHealthChecks(context.Background())[0]
		assert.Equal(t, "influx", report.Name)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, "Deactivated", report.Status)
		assert.Zero(t, report.Error)
	}

	// Jaeger.
	{
		var report = c.JaegerHealthChecks(context.Background())[0]
		assert.Equal(t, "jaeger", report.Name)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, "KO", report.Status)
		assert.Equal(t, "fail", report.Error)
	}

	// Redis.
	{
		var report = c.RedisHealthChecks(context.Background())[0]
		assert.Equal(t, "redis", report.Name)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, "Degraded", report.Status)
		assert.Equal(t, "fail", report.Error)
	}

	// Sentry.
	{
		var report = c.SentryHealthChecks(context.Background())[0]
		assert.Equal(t, "sentry", report.Name)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, "KO", report.Status)
		assert.Equal(t, "fail", report.Error)
	}

	// All.
	{
		var reply = c.AllHealthChecks(context.Background())
		assert.Equal(t, "Deactivated", reply["influx"])
		assert.Equal(t, "KO", reply["jaeger"])
		assert.Equal(t, "Degraded", reply["redis"])
		assert.Equal(t, "KO", reply["sentry"])
	}
}
