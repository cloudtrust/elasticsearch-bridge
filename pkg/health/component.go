package health

import (
	"context"

	common "github.com/cloudtrust/common-healthcheck"
)

// Status is the status of the health check.
type Status int

const (
	// OK is the status for a successful health check.
	OK Status = iota
	// KO is the status for an unsuccessful health check.
	KO
	// Degraded is the status for a degraded service, e.g. the service still works, but the metrics DB is KO.
	Degraded
	// Deactivated is the status for a service that is deactivated, e.g. we can disable error tracking, instrumenting, tracing,...
	Deactivated
)

func (s Status) String() string {
	var names = []string{"OK", "KO", "Degraded", "Deactivated"}

	if s < OK || s > Deactivated {
		return "Unknown"
	}

	return names[s]
}

// StorageModule is the interface of the module that stores the health reports
// in the DB.
type StorageModule interface {
	Update()
	Read()
}

// InfluxHealthChecker is the interface of the influx health check module.
type InfluxHealthChecker interface {
	HealthChecks(context.Context) []common.InfluxReport
}

// SentryHealthChecker is the interface of the sentry health check module.
type SentryHealthChecker interface {
	HealthChecks(context.Context) []common.SentryReport
}

// RedisHealthChecker is the interface of the redis health check module.
type RedisHealthChecker interface {
	HealthChecks(context.Context) []common.RedisReport
}

// JaegerHealthChecker is the interface of the jaeger health check module.
type JaegerHealthChecker interface {
	HealthChecks(context.Context) []common.JaegerReport
}

// Component is the Health component.
type Component struct {
	influx  InfluxHealthChecker
	jaeger  JaegerHealthChecker
	redis   RedisHealthChecker
	sentry  SentryHealthChecker
	storage StorageModule
}

// NewComponent returns the health component.
func NewComponent(influx InfluxHealthChecker, jaeger JaegerHealthChecker, redis RedisHealthChecker, sentry SentryHealthChecker, storage StorageModule) *Component {
	return &Component{
		influx:  influx,
		jaeger:  jaeger,
		redis:   redis,
		sentry:  sentry,
		storage: storage,
	}
}

type Report interface{}

// ExecInfluxHealthChecks executes the health checks for Influx.
func (c *Component) ExecInfluxHealthChecks(ctx context.Context) []common.InfluxReport {
	return c.influx.HealthChecks(ctx)
}

// ReadInfluxHealthChecks read the health checks status in DB.
func (c *Component) ReadInfluxHealthChecks(ctx context.Context) []common.InfluxReport {
	return nil
}

// ExecJaegerHealthChecks executes the health checks for Jaeger.
func (c *Component) ExecJaegerHealthChecks(ctx context.Context) []common.JaegerReport {
	return c.jaeger.HealthChecks(ctx)
}

// ReadJaegerHealthChecks read the health checks status in DB.
func (c *Component) ReadJaegerHealthChecks(ctx context.Context) []common.JaegerReport {
	return nil
}

// ExecRedisHealthChecks executes the health checks for Redis.
func (c *Component) ExecRedisHealthChecks(ctx context.Context) []common.RedisReport {
	return c.redis.HealthChecks(ctx)
}

// ReadRedisHealthChecks read the health checks status in DB.
func (c *Component) ReadRedisHealthChecks(ctx context.Context) []common.RedisReport {
	return nil
}

// ExecSentryHealthChecks executes the health checks for Sentry.
func (c *Component) ExecSentryHealthChecks(ctx context.Context) []common.SentryReport {
	return c.sentry.HealthChecks(ctx)
}

// ReadSentryHealthChecks read the health checks status in DB.
func (c *Component) ReadSentryHealthChecks(ctx context.Context) []common.SentryReport {
	return nil
}

// AllHealthChecks call all component checks and build a general health report.
func (c *Component) AllHealthChecks(ctx context.Context) map[string]string {
	var reports = map[string]string{}

	reports["influx"] = determineInfluxStatus(c.ExecInfluxHealthChecks(ctx))
	reports["jaeger"] = determineJaegerStatus(c.ExecJaegerHealthChecks(ctx))
	reports["redis"] = determineRedisStatus(c.ExecRedisHealthChecks(ctx))
	reports["sentry"] = determineSentryStatus(c.ExecSentryHealthChecks(ctx))

	return reports
}

// err return the string error that will be in the health report
func err(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// determineStatus parse all the tests reports and output a global status.
func determineInfluxStatus(reports []common.InfluxReport) string {
	var degraded = false
	for _, r := range reports {
		switch r.Status {
		case common.Deactivated:
			// If the status is Deactivated, we do not need to go through all tests reports, all
			// status will be the same.
			return Deactivated.String()
		case common.KO:
			return KO.String()
		case common.Degraded:
			degraded = true
		}
	}
	if degraded {
		return Degraded.String()
	}
	return OK.String()
}

// determineStatus parse all the tests reports and output a global status.
func determineRedisStatus(reports []common.RedisReport) string {
	var degraded = false
	for _, r := range reports {
		switch r.Status {
		case common.Deactivated:
			// If the status is Deactivated, we do not need to go through all tests reports, all
			// status will be the same.
			return Deactivated.String()
		case common.KO:
			return KO.String()
		case common.Degraded:
			degraded = true
		}
	}
	if degraded {
		return Degraded.String()
	}
	return OK.String()
}

// determineStatus parse all the tests reports and output a global status.
func determineJaegerStatus(reports []common.JaegerReport) string {
	var degraded = false
	for _, r := range reports {
		switch r.Status {
		case common.Deactivated:
			// If the status is Deactivated, we do not need to go through all tests reports, all
			// status will be the same.
			return Deactivated.String()
		case common.KO:
			return KO.String()
		case common.Degraded: 
			degraded = true
		}
	}
	if degraded {
		return Degraded.String()
	}
	return OK.String()
}

// determineStatus parse all the tests reports and output a global status.
func determineSentryStatus(reports []common.SentryReport) string {
	var degraded = false
	for _, r := range reports {
		switch r.Status {
		case common.Deactivated:
			// If the status is Deactivated, we do not need to go through all tests reports, all
			// status will be the same.
			return Deactivated.String()
		case common.KO:
			return KO.String()
		case common.Degraded:
			degraded = true
		}
	}
	if degraded {
		return Degraded.String()
	}
	return OK.String()
}