package health


import (
	"context"
	"math/rand"
	"time"
	"strconv"

	"github.com/go-kit/kit/endpoint"
	"github.com/cloudtrust/elasticsearch-bridge/api/fb"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/google/flatbuffers/go"
	otag "github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)

// IDGenerator is the interface of the distributed unique IDs generator.
type IDGenerator interface {
	NextValidID(context.Context) string
}


// MakeEndpointCorrelationIDMW makes a middleware that adds a correlation ID
// in the context if there is not already one.
func MakeEndpointCorrelationIDMW(flaki fb.FlakiClient, tracer opentracing.Tracer) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			var id = ctx.Value("correlation_id")

			if id == nil {
				if span := opentracing.SpanFromContext(ctx); span != nil {
					span = tracer.StartSpan("get_correlation_id", opentracing.ChildOf(span.Context()))
					otag.SpanKindRPCClient.Set(span)
					defer span.Finish()
					ctx = opentracing.ContextWithSpan(ctx, span)

					// Propagate the opentracing span.
					var carrier = make(opentracing.TextMapCarrier)
					var err = tracer.Inject(span.Context(), opentracing.TextMap, carrier)
					if err != nil {
						return nil, errors.Wrap(err, "could not inject tracer")
					}

					var md = metadata.New(carrier)
					ctx = metadata.NewOutgoingContext(ctx, md)
				}

				// Flaki request.
				var b = flatbuffers.NewBuilder(0)
				fb.FlakiRequestStart(b)
				b.Finish(fb.FlakiRequestEnd(b))

				var reply, err = flaki.NextValidID(ctx, b)
				var corrID string
				// If we cannot get ID from Flaki, we generate a random one.
				if err != nil {
					rand.Seed(time.Now().UnixNano())
					corrID = "degraded-" + strconv.FormatUint(rand.Uint64(), 10)
				} else {
					corrID = string(reply.Id())
				}

				ctx = context.WithValue(ctx, "correlation_id", corrID)
			}
			return next(ctx, req)
		}
	}
}
