package health

//go:generate mockgen -destination=./mock/elasticsearch.go -package=mock -mock_names=Elasticsearch=Elasticsearch  github.com/cloudtrust/elasticsearch-bridge/pkg/health Elasticsearch

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// ElasticsearchModule is the health check module for Elasticsearch.
type ElasticsearchModule struct {
	elasticsearch  elasticsearch
	enabled bool
}

// Elasticsearch is the interface of the Elasticsearch client.
type elasticsearch interface {
	Ping(timeout time.Duration) (time.Duration, string, error)
}

// NewInfluxModule returns the Elasticsearch health module.
func NewElasticsearchModule(elasticsearch elasticsearch, enabled bool) *ElasticsearchModule {
	return &ElasticsearchModule{
		elasticsearch:  elasticsearch,
		enabled: enabled,
	}
}

// ElasticsearchReport is the health report returned by the elasticsearch module.
type ElasticsearchReport struct {
	Name     string
	Duration time.Duration
	Status   Status
	Error    error
}

// HealthChecks executes all health checks for elasticsearch.
func (m *ElasticsearchModule) HealthChecks(context.Context) []ElasticsearchReport {
	var reports = []ElasticsearchReport{}
	reports = append(reports, m.elasticsearchPing())
	return reports
}

func (m *ElasticsearchModule) elasticsearchPing() ElasticsearchReport {
	var healthCheckName = "ping"

	if !m.enabled {
		return ElasticsearchReport{
			Name:   healthCheckName,
			Status: Deactivated,
		}
	}

	var now = time.Now()
	var _, _, err = m.elasticsearch.Ping(5 * time.Second)
	var duration = time.Since(now)

	var hcErr error
	var s Status
	switch {
	case err != nil:
		hcErr = errors.Wrap(err, "could not ping elasticsearch")
		s = KO
	default:
		s = OK 
	}

	return ElasticsearchReport{
		Name:     healthCheckName,
		Duration: duration,
		Status:   s,
		Error:    hcErr,
	}
}
