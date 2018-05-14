package health_test

import (
	"context"
	"testing"
	"time"

	. "github.com/cloudtrust/flaki-service/pkg/health"
	"github.com/cloudtrust/flaki-service/pkg/health/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestInfluxHealthCheckEndpoint(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var e = MakeInfluxHealthCheckEndpoint(mockComponent)

	// Health success.
	{ 
		mockComponent.EXPECT().InfluxHealthChecks(context.Background()).Return([]Report{{Name: "influx", Duration: (1 * time.Second).String(), Status: OK.String()}}).Times(1)
		var reports, err = e(context.Background(), nil)
		assert.Nil(t, err)
		var report = reports.([]Report)[0]
		assert.Equal(t, "influx", report.Name)
		assert.Equal(t, (1 * time.Second).String(), report.Duration)
		assert.Equal(t, "OK", report.Status)
		assert.Zero(t, report.Error)
	}

	// Health error.
	{
		mockComponent.EXPECT().InfluxHealthChecks(context.Background()).Return([]Report{{Name: "influx", Duration: (1 * time.Second).String(), Status: KO.String(), Error: "fail"}}).Times(1)
		var reports, err = e(context.Background(), nil)
		assert.Nil(t, err)
		var report = reports.([]Report)[0]
		assert.Equal(t, "influx", report.Name)
		assert.Equal(t, (1 * time.Second).String(), report.Duration)
		assert.Equal(t, "KO", report.Status)
		assert.Equal(t, "fail", report.Error)
	}
}

func TestJaegerHealthCheckEndpoint(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var e = MakeJaegerHealthCheckEndpoint(mockComponent)

	// Health success.
	{
		mockComponent.EXPECT().JaegerHealthChecks(context.Background()).Return([]Report{{Name: "jaeger", Duration: (1 * time.Second).String(), Status: OK.String()}}).Times(1)
		var reports, err = e(context.Background(), nil)
		assert.Nil(t, err)
		var report = reports.([]Report)[0]
		assert.Equal(t, "jaeger", report.Name)
		assert.Equal(t, (1 * time.Second).String(), report.Duration)
		assert.Equal(t, "OK", report.Status)
		assert.Zero(t, report.Error)
	}

	// Health error.
	{
		mockComponent.EXPECT().JaegerHealthChecks(context.Background()).Return([]Report{{Name: "jaeger", Duration: (1 * time.Second).String(), Status: KO.String(), Error: "fail"}}).Times(1)
		var reports, err = e(context.Background(), nil)
		assert.Nil(t, err)
		var report = reports.([]Report)[0]
		assert.Equal(t, "jaeger", report.Name)
		assert.Equal(t, (1 * time.Second).String(), report.Duration)
		assert.Equal(t, "KO", report.Status)
		assert.Equal(t, "fail", report.Error)
	}
}

func TestRedisHealthCheckEndpoint(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var e = MakeRedisHealthCheckEndpoint(mockComponent)

	// Health success.
	{
		mockComponent.EXPECT().RedisHealthChecks(context.Background()).Return([]Report{{Name: "redis", Duration: (1 * time.Second).String(), Status: OK.String()}}).Times(1)
		var reports, err = e(context.Background(), nil)
		assert.Nil(t, err)
		var report = reports.([]Report)[0]
		assert.Equal(t, "redis", report.Name)
		assert.Equal(t, (1 * time.Second).String(), report.Duration)
		assert.Equal(t, "OK", report.Status)
		assert.Zero(t, report.Error)
	}

	// Health error.
	{
		mockComponent.EXPECT().RedisHealthChecks(context.Background()).Return([]Report{{Name: "redis", Duration: (1 * time.Second).String(), Status: KO.String(), Error: "fail"}}).Times(1)
		var reports, err = e(context.Background(), nil)
		assert.Nil(t, err)
		var report = reports.([]Report)[0]
		assert.Equal(t, "redis", report.Name)
		assert.Equal(t, (1 * time.Second).String(), report.Duration)
		assert.Equal(t, "KO", report.Status)
		assert.Equal(t, "fail", report.Error)
	}
}
func TestSentryHealthCheckEndpoint(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var e = MakeSentryHealthCheckEndpoint(mockComponent)

	// Health success.
	{
		mockComponent.EXPECT().SentryHealthChecks(context.Background()).Return([]Report{{Name: "sentry", Duration: (1 * time.Second).String(), Status: OK.String()}}).Times(1)
		var reports, err = e(context.Background(), nil)
		assert.Nil(t, err)
		var report = reports.([]Report)[0]
		assert.Equal(t, "sentry", report.Name)
		assert.Equal(t, (1 * time.Second).String(), report.Duration)
		assert.Equal(t, "OK", report.Status)
		assert.Zero(t, report.Error)
	}

	// Health error.
	{
		mockComponent.EXPECT().SentryHealthChecks(context.Background()).Return([]Report{{Name: "sentry", Duration: (1 * time.Second).String(), Status: KO.String(), Error: "fail"}}).Times(1)
		var reports, err = e(context.Background(), nil)
		assert.Nil(t, err)
		var report = reports.([]Report)[0]
		assert.Equal(t, "sentry", report.Name)
		assert.Equal(t, (1 * time.Second).String(), report.Duration)
		assert.Equal(t, "KO", report.Status)
		assert.Equal(t, "fail", report.Error)
	}
}
