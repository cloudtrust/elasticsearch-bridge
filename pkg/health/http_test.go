package health_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	. "github.com/cloudtrust/flaki-service/pkg/health"
	"github.com/cloudtrust/flaki-service/pkg/health/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestInfluxHealthCheckHandler(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)
 
	var h = MakeInfluxHealthCheckHandler(MakeInfluxHealthCheckEndpoint(mockComponent))

	// Health success.
	mockComponent.EXPECT().InfluxHealthChecks(context.Background()).Return([]Report{{Name: "influx", Duration: (1 * time.Second).String(), Status: OK.String()}}).Times(1)

	// HTTP request.
	var req = httptest.NewRequest("GET", "http://cloudtrust.io/health/influx", nil)
	var w = httptest.NewRecorder()

	// Health check.
	h.ServeHTTP(w, req)
	var resp = w.Result()
	var body, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	var m = map[string]interface{}{}
	json.Unmarshal(body, &m)

	var r = m["health checks"].([]interface{})[0]
	{
		var m = r.(map[string]interface{})
		assert.Equal(t, "influx", m["name"])
		assert.Equal(t, (1 * time.Second).String(), m["duration"])
		assert.Equal(t, "OK", m["status"])
		assert.Zero(t, m["error"])
	}
}
func TestJaegerHealthCheckHandler(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var h = MakeJaegerHealthCheckHandler(MakeJaegerHealthCheckEndpoint(mockComponent))

	// Health success.
	mockComponent.EXPECT().JaegerHealthChecks(context.Background()).Return([]Report{{Name: "jaeger", Duration: (1 * time.Second).String(), Status: OK.String()}}).Times(1)

	// HTTP request.
	var req = httptest.NewRequest("GET", "http://cloudtrust.io/health/jaeger", nil)
	var w = httptest.NewRecorder()

	// Health check.
	h.ServeHTTP(w, req)
	var resp = w.Result()
	var body, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	var m = map[string]interface{}{}
	json.Unmarshal(body, &m)

	var r = m["health checks"].([]interface{})[0]
	{
		var m = r.(map[string]interface{})
		assert.Equal(t, "jaeger", m["name"])
		assert.Equal(t, (1 * time.Second).String(), m["duration"])
		assert.Equal(t, "OK", m["status"])
		assert.Zero(t, m["error"])
	}
}

func TestRedisHealthCheckHandler(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var h = MakeRedisHealthCheckHandler(MakeRedisHealthCheckEndpoint(mockComponent))

	// Health success.
	mockComponent.EXPECT().RedisHealthChecks(context.Background()).Return([]Report{{Name: "redis", Duration: (1 * time.Second).String(), Status: OK.String()}}).Times(1)

	// HTTP request.
	var req = httptest.NewRequest("GET", "http://cloudtrust.io/health/redis", nil)
	var w = httptest.NewRecorder()

	// Health check.
	h.ServeHTTP(w, req)
	var resp = w.Result()
	var body, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	var m = map[string]interface{}{}
	json.Unmarshal(body, &m)

	var r = m["health checks"].([]interface{})[0]
	{
		var m = r.(map[string]interface{})
		assert.Equal(t, "redis", m["name"])
		assert.Equal(t, (1 * time.Second).String(), m["duration"])
		assert.Equal(t, "OK", m["status"])
		assert.Zero(t, m["error"])
	}
}

func TestSentryHealthCheckHandler(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var h = MakeSentryHealthCheckHandler(MakeSentryHealthCheckEndpoint(mockComponent))

	// Health success.
	mockComponent.EXPECT().SentryHealthChecks(context.Background()).Return([]Report{{Name: "sentry", Duration: (1 * time.Second).String(), Status: OK.String()}}).Times(1)

	// HTTP request.
	var req = httptest.NewRequest("GET", "http://cloudtrust.io/health/sentry", nil)
	var w = httptest.NewRecorder()

	// Health check.
	h.ServeHTTP(w, req)
	var resp = w.Result()
	var body, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	var m = map[string]interface{}{}
	json.Unmarshal(body, &m)

	var r = m["health checks"].([]interface{})[0]
	{
		var m = r.(map[string]interface{})
		assert.Equal(t, "sentry", m["name"])
		assert.Equal(t, (1 * time.Second).String(), m["duration"])
		assert.Equal(t, "OK", m["status"])
		assert.Zero(t, m["error"])
	}
}

func TestHealthChecksHandler(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var h = MakeAllHealthChecksHandler(MakeAllHealthChecksEndpoint(mockComponent))

	// Health success.
	mockComponent.EXPECT().AllHealthChecks(context.Background()).Return(map[string]string{"influx": OK.String(), "jaeger": OK.String(), "redis": OK.String(), "sentry": OK.String()}).Times(1)

	// HTTP request.
	var req = httptest.NewRequest("GET", "http://cloudtrust.io/health", nil)
	var w = httptest.NewRecorder()

	// Health check.
	h.ServeHTTP(w, req)
	var resp = w.Result()
	var body, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	var m = map[string]string{}
	json.Unmarshal(body, &m)
	assert.Equal(t, "OK", m["influx"])
	assert.Equal(t, "OK", m["jaeger"])
	assert.Equal(t, "OK", m["redis"])
	assert.Equal(t, "OK", m["sentry"])
}
func TestHealthChecksHandlerFail(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()
	var mockComponent = mock.NewHealthChecker(mockCtrl)

	var h = MakeAllHealthChecksHandler(MakeAllHealthChecksEndpoint(mockComponent))

	// Health success.
	mockComponent.EXPECT().AllHealthChecks(context.Background()).Return(map[string]string{"influx": KO.String(), "jaeger": Deactivated.String(), "redis": Degraded.String(), "sentry": KO.String()}).Times(1)

	// HTTP request.
	var req = httptest.NewRequest("GET", "http://cloudtrust.io/health", nil)
	var w = httptest.NewRecorder()

	// Health check.
	h.ServeHTTP(w, req)
	var resp = w.Result()
	var body, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	var m = map[string]string{}
	json.Unmarshal(body, &m)
	assert.Equal(t, "KO", m["influx"])
	assert.Equal(t, "Deactivated", m["jaeger"])
	assert.Equal(t, "Degraded", m["redis"])
	assert.Equal(t, "KO", m["sentry"])
}

func TestHTTPErrorHandler(t *testing.T) {
	var e = func(ctx context.Context, request interface{}) (response interface{}, err error) {
		return nil, fmt.Errorf("fail")
	}

	var h = MakeInfluxHealthCheckHandler(e)

	// HTTP request.
	var req = httptest.NewRequest("GET", "http://cloudtrust.io/health/sentry", nil)
	var w = httptest.NewRecorder()

	// Health checks.
	h.ServeHTTP(w, req)
	var resp = w.Result()
	var body, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))
	// Decode JSON and check error value.
	{
		var m = map[string]string{}
		var err = json.Unmarshal(body, &m)
		assert.Nil(t, err)
		assert.Equal(t, "fail", m["error"])
	}
}
