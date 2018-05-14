package health

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/cloudtrust/common-healthcheck"

	"github.com/go-kit/kit/endpoint"
	http_transport "github.com/go-kit/kit/transport/http"
)

// MakeInfluxHealthCheckGETHandler makes a HTTP handler for the Influx HealthCheck endpoint.
func MakeInfluxHealthCheckGETHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeHealthCheckReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// MakeInfluxHealthCheckPOSTHandler makes a HTTP handler for the Influx HealthCheck endpoint.
func MakeInfluxHealthCheckPOSTHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeHealthCheckReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// MakeJaegerHealthCheckGETHandler makes a HTTP handler for the Jaeger HealthCheck endpoint.
func MakeJaegerHealthCheckGETHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeHealthCheckReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// MakeJaegerHealthCheckPOSTHandler makes a HTTP handler for the Jaeger HealthCheck endpoint. 
func MakeJaegerHealthCheckPOSTHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeHealthCheckReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// MakeRedisHealthCheckGETHandler makes a HTTP handler for the Redis HealthCheck endpoint.
func MakeRedisHealthCheckGETHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeHealthCheckReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// MakeRedisHealthCheckPOSTHandler makes a HTTP handler for the Redis HealthCheck endpoint.
func MakeRedisHealthCheckPOSTHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeHealthCheckReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// MakeSentryHealthCheckGETHandler makes a HTTP handler for the Sentry HealthCheck endpoint.
func MakeSentryHealthCheckGETHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeHealthCheckReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// MakeSentryHealthCheckPOSTHandler makes a HTTP handler for the Sentry HealthCheck endpoint.
func MakeSentryHealthCheckPOSTHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeHealthCheckReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// MakeAllHealthChecksHandler makes a HTTP handler for all health checks.
func MakeAllHealthChecksHandler(e endpoint.Endpoint) *http_transport.Server {
	return http_transport.NewServer(e,
		decodeHealthCheckRequest,
		encodeAllHealthChecksReply,
		http_transport.ServerErrorEncoder(healthCheckErrorHandler),
	)
}

// decodeHealthCheckRequest decodes the health check request.
func decodeHealthCheckRequest(_ context.Context, r *http.Request) (rep interface{}, err error) {
	return nil, nil
}

// reply contains all health check reports.
type reply struct {
	Reports []healthCheck `json:"health checks"`
}

// healthCheck is the result of a single healthcheck.
type healthCheck struct {
	Name     string `json:"name"`
	Duration string `json:"duration"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
}

// encodeHealthCheckReply encodes the health check reply.
func encodeHealthCheckReply(_ context.Context, w http.ResponseWriter, rep interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	var reply = reply{}

	switch reports := rep.(type) {
	case []common.InfluxReport:
		for _, r := range reports {
			reply.Reports = append(reply.Reports, healthCheck{
				Name: r.Name,
				Duration: r.Duration.String(),
				Status: r.Status.String(),
				Error: string(r.Error.Error()),
			})
		}
	case []common.JaegerReport:
		for _, r := range reports {
			reply.Reports = append(reply.Reports, healthCheck{
				Name: r.Name,
				Duration: r.Duration.String(),
				Status: r.Status.String(),
				Error: string(r.Error.Error()),
			})
		}
	case []common.RedisReport:
		for _, r := range reports {
			reply.Reports = append(reply.Reports, healthCheck{
				Name: r.Name,
				Duration: r.Duration.String(),
				Status: r.Status.String(),
				Error: string(r.Error.Error()),
			})
		}
	case []common.SentryReport:
		for _, r := range reports {
			reply.Reports = append(reply.Reports, healthCheck{
				Name: r.Name,
				Duration: r.Duration.String(),
				Status: r.Status.String(),
				Error: string(r.Error.Error()),
			})
		}
	}

	var data, err = json.MarshalIndent(reply, "", "  ")

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}

	return nil
}

// encodeAllHealthChecksReply encodes the health checks reply.
func encodeAllHealthChecksReply(_ context.Context, w http.ResponseWriter, rep interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	var reply = rep.(map[string]string)
	var data, err = json.MarshalIndent(reply, "", "  ")

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}

	return nil
}

// healthCheckErrorHandler encodes the health check reply when there is an error.
func healthCheckErrorHandler(ctx context.Context, err error, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	// Write error.
	var reply, _ = json.MarshalIndent(map[string]string{"error": err.Error()}, "", "  ")
	w.WriteHeader(http.StatusInternalServerError)
	w.Write(reply)
}
