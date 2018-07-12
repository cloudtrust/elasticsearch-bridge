package job

import (
	"fmt"
	"context"
	"time"
	"regexp"

	client "github.com/cloudtrust/elasticsearch-bridge/internal/elasticsearch_bridge"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/pkg/errors"
)

type ElasticsearchClient interface {
	ListIndexes() ([]client.IndexRepresentation, error)
	DeleteIndex(string) error
}

// MakeElasticsearchCleanIndexJob creates the job that periodically clean the indexes in ElasticSearch.
func MakeElasticsearchCleanIndexJob(elasticClient ElasticsearchClient, healthCheckValidity time.Duration) (*job.Job, error) {
	var listIndexes = func(ctx context.Context, r interface{}) (interface{}, error) {

		var indexes []client.IndexRepresentation
		var err error

		indexes, err = elasticClient.ListIndexes()

		// Chesk response status.
		if err != nil {
			return nil, errors.Wrap(err, "Cannot retrieve list of indexes from Elasticsearch")
		}

		var indexNames []string

		for _, index := range indexes {
			indexNames = append(indexNames, index.Index)
		}

		return indexNames, nil
	}

	var filterIndexesToClean = func(_ context.Context, r interface{}) (interface{}, error) {
		// Filter the indexes finishing with date format (YYYY.MM.DD)
		// Filter the list to keep only older than limit

		var indexes = r.([]string)
		var filteredIndexes []string
		var filterDate = time.Now().Add(-healthCheckValidity)

		for _, index := range indexes {
			re := regexp.MustCompile("[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$")

			if(re.MatchString(index)){
				var dateString = re.FindString(index)
				var t, err = time.Parse("2006.01.02", dateString)

				if err != nil {
					var s = err.Error()
					fmt.Print(s)
					break
				}

				if t.Before(filterDate){
					filteredIndexes = append(filteredIndexes, index)
				}
			}
		}

		return filteredIndexes, nil
	}

	var deleteIndexes = func(_ context.Context, r interface{}) (interface{}, error) {
		// Call API to clean all the index provided in param

		var indexes = r.([]string)

		for _, index := range indexes {
			var err = elasticClient.DeleteIndex(index)

			if err != nil {
				return nil, errors.Wrap(err, "Cannot delete index")
			}
		}

		return "ok", nil
	}
	return job.NewJob("elasticsearch-cleaning", job.Steps(listIndexes, filterIndexesToClean, deleteIndexes))
}