package health_test

//go:generate mockgen -destination=./mock/module.go -package=mock -mock_names=InfluxHealthChecker=InfluxHealthChecker,JaegerHealthChecker=JaegerHealthChecker,RedisHealthChecker=RedisHealthChecker,SentryHealthChecker=SentryHealthChecker,FlakiHealthChecker=FlakiHealthChecker,ElasticsearchHealthChecker=ElasticsearchHealthChecker,StoreModule=StoreModule  github.com/cloudtrust/elasticsearch-bridge/pkg/health InfluxHealthChecker,JaegerHealthChecker,RedisHealthChecker,SentryHealthChecker,FlakiHealthChecker,ElasticsearchHealthChecker,StoreModule

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	common "github.com/cloudtrust/common-healthcheck"
	"github.com/cloudtrust/elasticsearch-bridge/pkg/health"
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
	var mockFlakiModule = mock.NewFlakiHealthChecker(mockCtrl)
	var mockElasticsearchModule = mock.NewElasticsearchHealthChecker(mockCtrl)
	var mockStorage = mock.NewStoreModule(mockCtrl)
	var m = map[string]time.Duration{
		"influx":        1 * time.Minute,
		"jaeger":        1 * time.Minute,
		"redis":         1 * time.Minute,
		"sentry":        1 * time.Minute,
		"flaki":         1 * time.Minute,
		"elasticsearch": 1 * time.Minute,
	}

	var c = NewComponent(mockInfluxModule, mockJaegerModule, mockRedisModule, mockSentryModule, mockFlakiModule, mockElasticsearchModule, mockStorage, m)

	var (
		influxReports        = []common.InfluxReport{{Name: "influx", Duration: time.Duration(1 * time.Second), Status: common.OK}}
		jaegerReports        = []common.JaegerReport{{Name: "jaeger", Duration: time.Duration(1 * time.Second), Status: common.OK}}
		redisReports         = []common.RedisReport{{Name: "redis", Duration: time.Duration(1 * time.Second), Status: common.OK}}
		sentryReports        = []common.SentryReport{{Name: "sentry", Duration: time.Duration(1 * time.Second), Status: common.OK}}
		flakiReports         = []common.FlakiReport{{Name: "flaki", Duration: time.Duration(1 * time.Second), Status: common.OK}}
		elasticsearchReports = []health.ElasticsearchReport{{Name: "elasticsearch", Duration: time.Duration(1 * time.Second), Status: health.OK}}
		makeStoredReport     = func(name string) StoredReport {
			return StoredReport{
				ComponentID:     "000-000-000-00",
				ComponentName:   "elasticsearch-bridge",
				HealthcheckUnit: name,
				Reports:         json.RawMessage(`[{"name":"XXX", "status":"OK", "duration":"1s"}]`),
				LastUpdated:     time.Now(),
				ValidUntil:      time.Now().Add(1 * time.Hour),
			}
		}
	)

	// Influx.
	mockInfluxModule.EXPECT().HealthChecks(context.Background()).Return(influxReports).Times(1)
	mockStorage.EXPECT().Update("influx", m["influx"], gomock.Any()).Times(1)
	{
		var report = c.ExecInfluxHealthChecks(context.Background())
		//	var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"influx","duration":"1s","status":"OK","error":""}]`, string(report))
	}

	// Jaeger.
	mockJaegerModule.EXPECT().HealthChecks(context.Background()).Return(jaegerReports).Times(1)
	mockStorage.EXPECT().Update("jaeger", m["jaeger"], gomock.Any()).Times(1)
	{
		var report = c.ExecJaegerHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"jaeger","duration":"1s","status":"OK","error":""}]`, string(json))
	}

	// Redis.
	mockRedisModule.EXPECT().HealthChecks(context.Background()).Return(redisReports).Times(1)
	mockStorage.EXPECT().Update("redis", m["redis"], gomock.Any()).Times(1)
	{
		var report = c.ExecRedisHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"redis","duration":"1s","status":"OK","error":""}]`, string(json))

	}

	// Sentry.
	mockSentryModule.EXPECT().HealthChecks(context.Background()).Return(sentryReports).Times(1)
	mockStorage.EXPECT().Update("sentry", m["sentry"], gomock.Any()).Times(1)
	{
		var report = c.ExecSentryHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"sentry","duration":"1s","status":"OK","error":""}]`, string(json))
	}

	// Flaki.
	mockFlakiModule.EXPECT().HealthChecks(context.Background()).Return(flakiReports).Times(1)
	mockStorage.EXPECT().Update("flaki", m["flaki"], gomock.Any()).Times(1)
	{
		var report = c.ExecFlakiHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"flaki","duration":"1s","status":"OK","error":""}]`, string(json))
	}

	// Elasticsearch.
	mockElasticsearchModule.EXPECT().HealthChecks(context.Background()).Return(elasticsearchReports).Times(1)
	mockStorage.EXPECT().Update("elasticsearch", m["elasticsearch"], gomock.Any()).Times(1)
	{
		var report = c.ExecElasticsearchHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"elasticsearch","duration":"1s","status":"OK","error":""}]`, string(json))
	}

	// All.
	mockStorage.EXPECT().Read("influx").Return(makeStoredReport("influx"), nil).Times(1)
	mockStorage.EXPECT().Read("jaeger").Return(makeStoredReport("jaeger"), nil).Times(1)
	mockStorage.EXPECT().Read("redis").Return(makeStoredReport("redis"), nil).Times(1)
	mockStorage.EXPECT().Read("sentry").Return(makeStoredReport("sentry"), nil).Times(1)
	mockStorage.EXPECT().Read("flaki").Return(makeStoredReport("flaki"), nil).Times(1)
	mockStorage.EXPECT().Read("elasticsearch").Return(makeStoredReport("elasticsearch"), nil).Times(1)
	{
		var report = c.AllHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, "{\"elasticsearch\":[{\"name\":\"XXX\",\"status\":\"OK\",\"duration\":\"1s\"}],\"flaki\":[{\"name\":\"XXX\",\"status\":\"OK\",\"duration\":\"1s\"}],\"influx\":[{\"name\":\"XXX\",\"status\":\"OK\",\"duration\":\"1s\"}],\"jaeger\":[{\"name\":\"XXX\",\"status\":\"OK\",\"duration\":\"1s\"}],\"redis\":[{\"name\":\"XXX\",\"status\":\"OK\",\"duration\":\"1s\"}],\"sentry\":[{\"name\":\"XXX\",\"status\":\"OK\",\"duration\":\"1s\"}]}", string(json))
	}

}

func TestHealthChecksFail(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockInfluxModule = mock.NewInfluxHealthChecker(mockCtrl)
	var mockJaegerModule = mock.NewJaegerHealthChecker(mockCtrl)
	var mockRedisModule = mock.NewRedisHealthChecker(mockCtrl)
	var mockSentryModule = mock.NewSentryHealthChecker(mockCtrl)
	var mockFlakiModule = mock.NewFlakiHealthChecker(mockCtrl)
	var mockElasticsearchModule = mock.NewElasticsearchHealthChecker(mockCtrl)
	var mockStorage = mock.NewStoreModule(mockCtrl)
	var m = map[string]time.Duration{
		"influx":        1 * time.Minute,
		"jaeger":        1 * time.Minute,
		"redis":         1 * time.Minute,
		"sentry":        1 * time.Minute,
		"flaki":         1 * time.Minute,
		"elasticsearch": 1 * time.Minute,
	}

	var c = NewComponent(mockInfluxModule, mockJaegerModule, mockRedisModule, mockSentryModule, mockFlakiModule, mockElasticsearchModule, mockStorage, m)

	var (
		influxReports        = []common.InfluxReport{{Name: "influx", Duration: time.Duration(1 * time.Second), Status: common.Deactivated}}
		jaegerReports        = []common.JaegerReport{{Name: "jaeger", Duration: time.Duration(1 * time.Second), Status: common.KO, Error: fmt.Errorf("fail")}}
		redisReports         = []common.RedisReport{{Name: "redis", Duration: time.Duration(1 * time.Second), Status: common.Degraded, Error: fmt.Errorf("fail")}}
		sentryReports        = []common.SentryReport{{Name: "sentry", Duration: time.Duration(1 * time.Second), Status: common.KO, Error: fmt.Errorf("fail")}}
		flakiReports         = []common.FlakiReport{{Name: "flaki", Duration: time.Duration(1 * time.Second), Status: common.KO, Error: fmt.Errorf("fail")}}
		elasticsearchReports = []health.ElasticsearchReport{{Name: "elasticsearch", Duration: time.Duration(1 * time.Second), Status: health.KO, Error: fmt.Errorf("fail")}}
		makeStoredReport     = func(name string) StoredReport {
			return StoredReport{
				ComponentID:     "000-000-000-00",
				ComponentName:   "elasticsearch-bridge",
				HealthcheckUnit: name,
				Reports:         json.RawMessage(`[{"name":"XXX", "status":"OK", "duration":"1s"}]`),
				LastUpdated:     time.Now(),
				ValidUntil:      time.Now().Add(1 * time.Hour),
			}
		}
	)

	// Influx.
	mockInfluxModule.EXPECT().HealthChecks(context.Background()).Return(influxReports).Times(1)
	mockStorage.EXPECT().Update("influx", m["influx"], gomock.Any()).Times(1)
	{
		var report = c.ExecInfluxHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"influx","duration":"1s","status":"Deactivated","error":""}]`, string(json))
	}

	// Jaeger.
	mockJaegerModule.EXPECT().HealthChecks(context.Background()).Return(jaegerReports).Times(1)
	mockStorage.EXPECT().Update("jaeger", m["jaeger"], gomock.Any()).Times(1)
	{
		var report = c.ExecJaegerHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"jaeger","duration":"1s","status":"KO","error":"fail"}]`, string(json))
	}

	// Redis.
	mockRedisModule.EXPECT().HealthChecks(context.Background()).Return(redisReports).Times(1)
	mockStorage.EXPECT().Update("redis", m["redis"], gomock.Any()).Times(1)
	{
		var report = c.ExecRedisHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"redis","duration":"1s","status":"Degraded","error":"fail"}]`, string(json))
	}

	// Sentry.
	mockSentryModule.EXPECT().HealthChecks(context.Background()).Return(sentryReports).Times(1)
	mockStorage.EXPECT().Update("sentry", m["sentry"], gomock.Any()).Times(1)
	{
		var report = c.ExecSentryHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"sentry","duration":"1s","status":"KO","error":"fail"}]`, string(json))
	}

	// Flaki.
	mockFlakiModule.EXPECT().HealthChecks(context.Background()).Return(flakiReports).Times(1)
	mockStorage.EXPECT().Update("flaki", m["flaki"], gomock.Any()).Times(1)
	{
		var report = c.ExecFlakiHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"flaki","duration":"1s","status":"KO","error":"fail"}]`, string(json))
	}

	// Elasticsearch.
	mockElasticsearchModule.EXPECT().HealthChecks(context.Background()).Return(elasticsearchReports).Times(1)
	mockStorage.EXPECT().Update("elasticsearch", m["elasticsearch"], gomock.Any()).Times(1)
	{
		var report = c.ExecElasticsearchHealthChecks(context.Background())
		var json, _ = json.Marshal(&report)
		assert.Equal(t, `[{"name":"elasticsearch","duration":"1s","status":"KO","error":"fail"}]`, string(json))
	}

	// All.
	mockStorage.EXPECT().Read("influx").Return(makeStoredReport("influx"), nil).Times(1)
	mockStorage.EXPECT().Read("jaeger").Return(makeStoredReport("jaeger"), nil).Times(1)
	mockStorage.EXPECT().Read("redis").Return(makeStoredReport("redis"), nil).Times(1)
	mockStorage.EXPECT().Read("sentry").Return(makeStoredReport("sentry"), nil).Times(1)
	mockStorage.EXPECT().Read("flaki").Return(makeStoredReport("flaki"), nil).Times(1)
	mockStorage.EXPECT().Read("elasticsearch").Return(makeStoredReport("elasticsearch"), nil).Times(1)
	{
		var reply = c.AllHealthChecks(context.Background())
		var m map[string]json.RawMessage
		json.Unmarshal(reply, &m)

		assert.Equal(t, `[{"name":"XXX","status":"OK","duration":"1s"}]`, string(m["influx"]))
		assert.Equal(t, `[{"name":"XXX","status":"OK","duration":"1s"}]`, string(m["jaeger"]))
		assert.Equal(t, `[{"name":"XXX","status":"OK","duration":"1s"}]`, string(m["redis"]))
		assert.Equal(t, `[{"name":"XXX","status":"OK","duration":"1s"}]`, string(m["sentry"]))
		assert.Equal(t, `[{"name":"XXX","status":"OK","duration":"1s"}]`, string(m["flaki"]))
		assert.Equal(t, `[{"name":"XXX","status":"OK","duration":"1s"}]`, string(m["elasticsearch"]))
	}
}
