package health

import (
	"context"
	"encoding/json"
	"time"

	client "github.com/cloudtrust/elasticsearch-bridge/internal/elasticsearch_bridge"
	"github.com/go-kit/kit/log"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// ElasticsearchModule is the health check module for Elasticsearch.
type ElasticsearchModule struct {
	elasticsearchClient ElasticsearchClient
}

// ElasticsearchClient is the interface of Elasticsearch.
type ElasticsearchClient interface {
	ListIndexes() ([]client.IndexRepresentation, error)
	GetIndex(indexName string) (client.IndexSettingsRepresentation, error)
	CreateIndex(indexName string) error
	DeleteIndex(indexName string) error
	Health() (client.HealthRepresentation, error)
}

// NewElasticsearchModule returns the Elasticsearch health module.
func NewElasticsearchModule(client ElasticsearchClient) *ElasticsearchModule {
	return &ElasticsearchModule{
		elasticsearchClient: client,
	}
}

// ElasticsearchReport is the health report returned by the Elasticsearch module.
type ElasticsearchReport struct {
	Name     string
	Duration time.Duration
	Status   Status
	Error    error
	Infos    json.RawMessage
}

func (i *ElasticsearchReport) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name     string          `json:"name"`
		Duration string          `json:"duration"`
		Status   string          `json:"status"`
		Error    string          `json:"error"`
		Infos    json.RawMessage `json:"infos,omitempty"`
	}{
		Name:     i.Name,
		Duration: i.Duration.String(),
		Status:   i.Status.String(),
		Error:    err(i.Error),
		Infos:    i.Infos,
	})
}

// HealthChecks executes all health checks for Flaki.
func (m *ElasticsearchModule) HealthChecks(context.Context) []ElasticsearchReport {
	var reports = []ElasticsearchReport{}
	reports = append(reports, m.elasticsearchHealthCheck())
	reports = append(reports, m.elasticsearchIndexCheck())
	return reports
}

func (m *ElasticsearchModule) elasticsearchHealthCheck() ElasticsearchReport {
	var healthCheckName = "Health"

	var now = time.Now()
	var health, err = m.elasticsearchClient.Health()
	var duration = time.Since(now)

	var hcErr error
	var s Status
	switch {
	case err != nil:
		hcErr = errors.Wrap(err, "could not check health of cluster")
		s = KO
	default:
		switch health.Status {
		case "yellow":
			s = Degraded
		case "green":
			s = OK
		case "red":
			s = KO
		default:
			s = Unknown
		}
	}

	var jsonInfos, _ = json.Marshal(health)

	return ElasticsearchReport{
		Name:     healthCheckName,
		Duration: duration,
		Status:   s,
		Error:    hcErr,
		Infos:    jsonInfos,
	}
}

func (m *ElasticsearchModule) elasticsearchIndexCheck() ElasticsearchReport {
	var healthCheckName = "Index API"

	var UUID string
	{
		var UUIDgen, _ = uuid.NewUUID()
		UUID = UUIDgen.String()
	}

	// query flaki next valid ID
	var now = time.Now()

	var _, err = m.elasticsearchClient.ListIndexes()

	if err != nil {
		var duration = time.Since(now)
		var hcErr = errors.Wrap(err, "could not query elsaticsearch")
		return ElasticsearchReport{
			Name:     healthCheckName,
			Duration: duration,
			Status:   KO,
			Error:    hcErr,
		}
	}

	err = m.elasticsearchClient.CreateIndex(UUID)

	if err != nil {
		var duration = time.Since(now)
		var hcErr = errors.Wrap(err, "could not add index in elsaticsearch")
		return ElasticsearchReport{
			Name:     healthCheckName,
			Duration: duration,
			Status:   KO,
			Error:    hcErr,
		}
	}

	err = m.elasticsearchClient.DeleteIndex(UUID)
	var duration = time.Since(now)

	var hcErr error
	var s Status
	switch {
	case err != nil:
		hcErr = errors.Wrap(err, "could not query flaki service")
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

// IElasticsearchHealthChecker is the interface of the elasticsearch health check module.
type IElasticsearchHealthChecker interface {
	HealthChecks(context.Context) []ElasticsearchReport
}

// Logging middleware at module level.
type elasticsearchModuleLoggingMW struct {
	logger log.Logger
	next   IElasticsearchHealthChecker
}

// MakeElasticsearchModuleLoggingMW makes a logging middleware at module level.
func MakeElasticsearchModuleLoggingMW(logger log.Logger) func(IElasticsearchHealthChecker) IElasticsearchHealthChecker {
	return func(next IElasticsearchHealthChecker) IElasticsearchHealthChecker {
		return &elasticsearchModuleLoggingMW{
			logger: logger,
			next:   next,
		}
	}
}

// elasticsearchModuleLoggingMW implements Module.
func (m *elasticsearchModuleLoggingMW) HealthChecks(ctx context.Context) []ElasticsearchReport {
	defer func(begin time.Time) {
		m.logger.Log("unit", "HealthChecks", "correlation_id", ctx.Value("correlation_id").(string), "took", time.Since(begin))
	}(time.Now())

	return m.next.HealthChecks(ctx)
}
