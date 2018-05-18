package elasticsearch_bridge

import (
	"context"

	fb_flaki "github.com/cloudtrust/elasticsearch-bridge/api/fb"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
)

type FlakiLightClient struct {
	flakiClient fb_flaki.FlakiClient
}

func NewFlakiLightClient(client fb_flaki.FlakiClient) *FlakiLightClient {
	return &FlakiLightClient{
		flakiClient: client,
	}
}

func (f *FlakiLightClient) NextValidID() (string, error) {
	var b = flatbuffers.NewBuilder(0)
	fb_flaki.FlakiRequestStart(b)
	b.Finish(fb_flaki.FlakiRequestEnd(b))

	var reply, err = f.flakiClient.NextValidID(context.Background(), b)

	if err != nil {
		return "", errors.Wrapf(err, "cannot get ID from flaki-service")
	}

	return string(reply.Id()), nil
}
