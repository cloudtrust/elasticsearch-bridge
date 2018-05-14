package health

//go:generate mockgen -destination=./mock/idGenerator.go -package=mock -mock_names=IDGeneratorModule=FlakiModule github.com/cloudtrust/flaki-service/pkg/flaki IDGeneratorModule

import (
	"context"

	"github.com/go-kit/kit/endpoint"
)

// IDGenerator is the interface of the distributed unique IDs generator.
type IDGenerator interface {
	NextValidID(context.Context) string
}

// MakeEndpointCorrelationIDMW makes a middleware that adds a correlation ID
// in the context if there is not already one.
func MakeEndpointCorrelationIDMW(g IDGenerator) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			var id = ctx.Value("correlation_id")

			// If there is no correlation ID in the context, request one.
			if id == nil {
				ctx = context.WithValue(ctx, "correlation_id", g.NextValidID(ctx))
			}
			return next(ctx, req)
		} 
	}
}
