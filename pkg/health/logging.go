package health

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
)

// MakeEndpointLoggingMW makes a logging middleware.
func MakeEndpointLoggingMW(logger log.Logger) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			defer func(begin time.Time) {
				logger.Log("correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
			}(time.Now())

			return next(ctx, req)
		}
	}
}

// Logging middleware at component level.
type componentLoggingMW struct {
	logger log.Logger
	next   HealthChecker
}

// MakeComponentLoggingMW makes a logging middleware at component level.
func MakeComponentLoggingMW(logger log.Logger) func(HealthChecker) HealthChecker {
	return func(next HealthChecker) HealthChecker {
		return &componentLoggingMW{
			logger: logger,
			next:   next,
		}
	}
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ExecInfluxHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ExecInfluxHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ExecInfluxHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ReadInfluxHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ReadInfluxHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ReadInfluxHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ExecJaegerHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ExecJaegerHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ExecJaegerHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ReadJaegerHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ReadJaegerHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ReadJaegerHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ExecRedisHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ExecRedisHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ExecRedisHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ReadRedisHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ReadRedisHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ReadRedisHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ExecSentryHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ExecSentryHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ExecSentryHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ReadSentryHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ReadSentryHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ReadSentryHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ExecFlakiHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ExecFlakiHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ExecFlakiHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ReadFlakiHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ReadFlakiHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ReadFlakiHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ExecElasticsearchHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ExecElasticsearchHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ExecElasticsearchHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) ReadElasticsearchHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "ReadElasticsearchHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.ReadElasticsearchHealthChecks(ctx)
}

// componentLoggingMW implements Component.
func (m *componentLoggingMW) AllHealthChecks(ctx context.Context) json.RawMessage {
	defer func(begin time.Time) {
		m.logger.Log("unit", "AllHealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.AllHealthChecks(ctx)
}
