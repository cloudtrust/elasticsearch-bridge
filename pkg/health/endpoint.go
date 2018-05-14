package health

//go:generate mockgen -destination=./mock/component.go -package=mock -mock_names=HealthChecker=HealthChecker github.com/cloudtrust/flaki-service/pkg/health HealthChecker

import (
	"context"

	"github.com/go-kit/kit/endpoint"
)

// Endpoints wraps a service behind a set of endpoints.
type Endpoints struct {
	InfluxExecHealthCheck endpoint.Endpoint
	InfluxReadHealthCheck endpoint.Endpoint
	JaegerExecHealthCheck endpoint.Endpoint
	JaegerReadHealthCheck endpoint.Endpoint
	RedisExecHealthCheck  endpoint.Endpoint
	RedisReadHealthCheck  endpoint.Endpoint
	SentryExecHealthCheck endpoint.Endpoint
	SentryReadHealthCheck endpoint.Endpoint
	AllHealthChecks       endpoint.Endpoint
}

// HealthChecker is the health component interface.
type HealthChecker interface {
	ExecInfluxHealthChecks(context.Context) []Report 
	ReadInfluxHealthChecks(context.Context) []Report
	ExecJaegerHealthChecks(context.Context) []Report
	ReadJaegerHealthChecks(context.Context) []Report
	ExecRedisHealthChecks(context.Context) []Report
	ReadRedisHealthChecks(context.Context) []Report
	ExecSentryHealthChecks(context.Context) []Report
	ReadSentryHealthChecks(context.Context) []Report
	AllHealthChecks(context.Context) map[string]string
}

// MakeExecInfluxHealthCheckEndpoint makes the InfluxHealthCheck endpoint
// that forces the execution of the health checks.
func MakeExecInfluxHealthCheckEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.ExecInfluxHealthChecks(ctx), nil
	}
}

// MakeReadInfluxHealthCheckEndpoint makes the InfluxHealthCheck endpoint
// that read the last health check status in DB.
func MakeReadInfluxHealthCheckEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.ReadInfluxHealthChecks(ctx), nil
	}
}

// MakeExecJaegerHealthCheckEndpoint makes the JaegerHealthCheck endpoint
// that forces the execution of the health checks.
func MakeExecJaegerHealthCheckEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.ExecJaegerHealthChecks(ctx), nil
	}
}

// MakeReadJaegerHealthCheckEndpoint makes the JaegerHealthCheck endpoint
// that read the last health check status in DB.
func MakeReadJaegerHealthCheckEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.ReadJaegerHealthChecks(ctx), nil
	}
}

// MakeExecRedisHealthCheckEndpoint makes the RedisHealthCheck endpoint
// that forces the execution of the health checks.
func MakeExecRedisHealthCheckEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.ExecRedisHealthChecks(ctx), nil
	}
}

// MakeReadRedisHealthCheckEndpoint makes the RedisHealthCheck endpoint
// that read the last health check status in DB.
func MakeReadRedisHealthCheckEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.ReadRedisHealthChecks(ctx), nil
	}
}

// MakeExecSentryHealthCheckEndpoint makes the SentryHealthCheck endpoint
// that forces the execution of the health checks.
func MakeExecSentryHealthCheckEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.ExecSentryHealthChecks(ctx), nil
	}
}

// MakeReadSentryHealthCheckEndpoint makes the SentryHealthCheck endpoint
// that read the last health check status in DB.
func MakeReadSentryHealthCheckEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.ReadSentryHealthChecks(ctx), nil
	}
}

// MakeAllHealthChecksEndpoint makes an endpoint that does all health checks.
func MakeAllHealthChecksEndpoint(hc HealthChecker) endpoint.Endpoint {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return hc.AllHealthChecks(ctx), nil
	}
}
