package elasticsearch_bridge

import (
	"encoding/json"
	"fmt"
	net_url "net/url"
	"time"

	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugin"
	"gopkg.in/h2non/gentleman.v2/plugins/query"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"
	"gopkg.in/h2non/gentleman.v2/plugins/url"
)

const (
	listIndexes = "/_cat/indices"
	health      = "/_cluster/health"
)

type Config struct {
	Addr    string
	Timeout time.Duration
}

type Client struct {
	httpClient *gentleman.Client
}

type IndexSettingsRepresentation struct {
	CreationDate     time.Time `json:"creation_date"`
	NumberOfShards   int       `json:"number_of_shards"`
	NumberOfReplicas int       `json:"number_of_replicas"`
	UUID             string    `json:"uuid"`
}

type IndexRepresentation struct {
	Index  string `json:"index"`
	Status string `json:"status"`
}

type HealthRepresentation struct {
	ClusterName                 string  `json:"cluster_name"`
	Status                      string  `json:"status"`
	TimedOut                    bool    `json:"timed_out"`
	NumberOfNodes               int     `json:"number_of_nodes"`
	NumberOfDataNodes           int     `json:"number_of_data_nodes"`
	ActivedPrimaryShards        int    `json:"active_primary_shards"`
	ActiveShards                int     `json:"active_shards"`
	RelocatingShards            int     `json:"relocating_shards"`
	InitializingShards          int     `json:"initializing_shards"`
	UnassignedShards            int     `json:"unassigned_shards"`
	DelayedUnassignedShards     int     `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks        int     `json:"number_of_pending_tasks"`
	NumberOfInFlightFetch       int     `json:"number_of_in_flight_fetch"`
	TaskMaxWaitingInQueueMillis int     `json:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercentAsNumber float64 `json:"active_shards_percent_as_number"`
}

func New(config Config) (*Client, error) {
	var u *net_url.URL
	{
		var err error
		u, err = net_url.Parse(config.Addr)
		if err != nil {
			return nil, fmt.Errorf("could not parse URL: %v", err)
		}
	}

	var httpClient = gentleman.New()
	{
		httpClient = httpClient.URL(u.String())
		httpClient = httpClient.Use(timeout.Request(config.Timeout))
	}

	return &Client{
		httpClient: httpClient,
	}, nil
}

func (c *Client) Health() (HealthRepresentation, error) {
	var resp = HealthRepresentation{}
	var err = c.get(&resp, url.Path(health))

	return resp, err
}

func (c *Client) ListIndexes() ([]IndexRepresentation, error) {
	var resp []IndexRepresentation
	var plugins = createQueryPlugins("format", "json")
	plugins = append(plugins, url.Path(listIndexes))
	var err = c.get(&resp, plugins...)
	return resp, err
}

func (c *Client) GetIndex(indexName string) (IndexSettingsRepresentation, error) {
	var resp = IndexSettingsRepresentation{}
	var err = c.get(&resp, url.Path(indexName))
	return resp, err
}

func (c *Client) CreateIndex(indexName string) error {
	return c.put(url.Path("/" + indexName))
}

func (c *Client) DeleteIndex(indexName string) error {
	return c.delete(url.Path(indexName))
}

// get is a HTTP get method.
func (c *Client) get(data interface{}, plugins ...plugin.Plugin) error {
	var req = c.httpClient.Get()
	req = applyPlugins(req, "", plugins...)

	var resp *gentleman.Response
	{
		var err error
		resp, err = req.Do()
		if err != nil {
			return fmt.Errorf("could not get response: %v", err)
		}

		switch {
		case resp.StatusCode >= 400:
			return fmt.Errorf("invalid status code: '%v': %v", resp.RawResponse.Status, string(resp.Bytes()))
		case resp.StatusCode >= 200:
			return json.Unmarshal(resp.Bytes(), data)
		default:
			return fmt.Errorf("unknown response status code: %v", resp.StatusCode)
		}
	}
}

func (c *Client) post(plugins ...plugin.Plugin) error {
	var req = c.httpClient.Post()
	req = applyPlugins(req, "", plugins...)

	var resp *gentleman.Response
	{
		var err error
		resp, err = req.Do()
		if err != nil {
			return fmt.Errorf("could not get response: %v", err)
		}

		switch {
		case resp.StatusCode >= 400:
			return fmt.Errorf("invalid status code: '%v': %v", resp.RawResponse.Status, string(resp.Bytes()))
		case resp.StatusCode >= 200:
			return nil
		default:
			return fmt.Errorf("unknown response status code: %v", resp.StatusCode)
		}
	}
}

func (c *Client) delete(plugins ...plugin.Plugin) error {
	var req = c.httpClient.Delete()
	req = applyPlugins(req, "", plugins...)

	var resp *gentleman.Response
	{
		var err error
		resp, err = req.Do()
		if err != nil {
			return fmt.Errorf("could not get response: %v", err)
		}

		switch {
		case resp.StatusCode >= 400:
			return fmt.Errorf("invalid status code: '%v': %v", resp.RawResponse.Status, string(resp.Bytes()))
		case resp.StatusCode >= 200:
			return nil
		default:
			return fmt.Errorf("unknown response status code: %v", resp.StatusCode)
		}
	}
}

func (c *Client) put(plugins ...plugin.Plugin) error {
	var req = c.httpClient.Put()
	req = applyPlugins(req, "", plugins...)

	var resp *gentleman.Response
	{
		var err error
		resp, err = req.Do()
		if err != nil {
			return fmt.Errorf("could not get response: %v", err)
		}

		switch {
		case resp.StatusCode >= 400:
			return fmt.Errorf("invalid status code: '%v': %v", resp.RawResponse.Status, string(resp.Bytes()))
		case resp.StatusCode >= 200:
			return nil
		default:
			return fmt.Errorf("unknown response status code: %v", resp.StatusCode)
		}
	}
}

// applyPlugins apply all the plugins to the request req.
func applyPlugins(req *gentleman.Request, accessToken string, plugins ...plugin.Plugin) *gentleman.Request {
	var r = req.SetHeader("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	for _, p := range plugins {
		r = r.Use(p)
	}
	return r
}

// createQueryPlugins create query parameters with the key values paramKV.
func createQueryPlugins(paramKV ...string) []plugin.Plugin {
	var plugins = []plugin.Plugin{}
	for i := 0; i < len(paramKV); i += 2 {
		var k = paramKV[i]
		var v = paramKV[i+1]
		plugins = append(plugins, query.Set(k, v))
	}
	return plugins
}
