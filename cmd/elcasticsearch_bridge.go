package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	common "github.com/cloudtrust/common-healthcheck"
	fb_flaki "github.com/cloudtrust/elasticsearch-bridge/api/fb"
	elasticsearch_bridge "github.com/cloudtrust/elasticsearch-bridge/internal/elasticsearch_bridge"
	"github.com/cloudtrust/elasticsearch-bridge/pkg/health"
	health_job "github.com/cloudtrust/elasticsearch-bridge/pkg/job"
	"github.com/cloudtrust/go-jobs"
	"github.com/cloudtrust/go-jobs/job"
	job_lock "github.com/cloudtrust/go-jobs/lock"
	job_status "github.com/cloudtrust/go-jobs/status"
	"github.com/coreos/go-systemd/dbus"
	"github.com/garyburd/redigo/redis"
	sentry "github.com/getsentry/raven-go"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	gokit_influx "github.com/go-kit/kit/metrics/influx"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gorilla/mux"
	influx "github.com/influxdata/influxdb/client/v2"
	_ "github.com/lib/pq"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	jaeger "github.com/uber/jaeger-client-go/config"
	"google.golang.org/grpc"
)

var (
	// ComponentName is the name of the component.
	ComponentName = "elasticsearch-bridge"
	// ComponentID is an unique ID generated by Flaki at component startup.
	ComponentID = "unknown"
	// Version is filled by the compiler.
	Version = "unknown"
	// Environment is filled by the compiler.
	Environment = "unknown"
	// GitCommit is filled by the compiler.
	GitCommit = "unknown"
)

const (
	influxKey        = "influx"
	jaegerKey        = "jaeger"
	redisKey         = "redis"
	sentryKey        = "sentry"
	flakiKey         = "flaki"
	elasticsearchKey = "elasticsearch"
)

func main() {

	// Logger.
	var logger = log.NewJSONLogger(os.Stdout)
	{
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}
	defer logger.Log("msg", "goodbye")

	// Configurations.
	var c = config(log.With(logger, "unit", "config"))
	var (
		// Component
		httpAddr = c.GetString("component-http-host-port")

		// Flaki
		flakiAddr = c.GetString("flaki-host-port")

		// Elasticsearch
		elasticsearchConfig = elasticsearch_bridge.Config{
			Addr: fmt.Sprintf("http://%s", c.GetString("elasticsearch-host-port")),
		}
		elasticsearchIndexCleanInterval = c.GetDuration("elasticsearch-index-clean-interval")
		elasticsearchIndexExpiration    = c.GetDuration("elasticsearch-index-expiration")

		// Enabled units
		cockroachEnabled  = c.GetBool("cockroach")
		influxEnabled     = c.GetBool("influx")
		jaegerEnabled     = c.GetBool("jaeger")
		redisEnabled      = c.GetBool("redis")
		sentryEnabled     = c.GetBool("sentry")
		pprofRouteEnabled = c.GetBool("pprof-route-enabled")

		// Influx
		influxHTTPConfig = influx.HTTPConfig{
			Addr:     fmt.Sprintf("http://%s", c.GetString("influx-host-port")),
			Username: c.GetString("influx-username"),
			Password: c.GetString("influx-password"),
		}
		influxBatchPointsConfig = influx.BatchPointsConfig{
			Precision:        c.GetString("influx-precision"),
			Database:         c.GetString("influx-database"),
			RetentionPolicy:  c.GetString("influx-retention-policy"),
			WriteConsistency: c.GetString("influx-write-consistency"),
		}
		influxWriteInterval = c.GetDuration("influx-write-interval")

		// Jaeger
		jaegerConfig = jaeger.Configuration{
			Disabled: !jaegerEnabled,
			Sampler: &jaeger.SamplerConfig{
				Type:              c.GetString("jaeger-sampler-type"),
				Param:             c.GetFloat64("jaeger-sampler-param"),
				SamplingServerURL: fmt.Sprintf("http://%s", c.GetString("jaeger-sampler-host-port")),
			},
			Reporter: &jaeger.ReporterConfig{
				LogSpans:            c.GetBool("jaeger-reporter-logspan"),
				BufferFlushInterval: c.GetDuration("jaeger-write-interval"),
			},
		}
		jaegerCollectorHealthcheckURL = c.GetString("jaeger-collector-healthcheck-host-port")

		// Sentry
		sentryDSN = c.GetString("sentry-dsn")

		// Redis
		redisURL           = c.GetString("redis-host-port")
		redisPassword      = c.GetString("redis-password")
		redisDatabase      = c.GetInt("redis-database")
		redisWriteInterval = c.GetDuration("redis-write-interval")

		// Cockroach
		cockroachHostPort      = c.GetString("cockroach-host-port")
		cockroachUsername      = c.GetString("cockroach-username")
		cockroachPassword      = c.GetString("cockroach-password")
		cockroachHealthDB      = c.GetString("cockroach-health-database")
		cockroachJobsDB        = c.GetString("cockroach-jobs-database")
		cockroachCleanInterval = c.GetDuration("cockroach-clean-interval")

		// Jobs
		healthChecksValidity = map[string]time.Duration{
			influxKey:        c.GetDuration("job-influx-health-validity"),
			jaegerKey:        c.GetDuration("job-jaeger-health-validity"),
			redisKey:         c.GetDuration("job-redis-health-validity"),
			sentryKey:        c.GetDuration("job-sentry-health-validity"),
			flakiKey:         c.GetDuration("job-flaki-health-validity"),
			elasticsearchKey: c.GetDuration("job-elasticsearch-health-validity"),
		}
	)

	// Redis.
	type Redis interface {
		Close() error
		Do(commandName string, args ...interface{}) (reply interface{}, err error)
		Send(commandName string, args ...interface{}) error
		Flush() error
	}

	var redisClient Redis = &elasticsearch_bridge.NoopRedis{}
	if redisEnabled {
		var err error
		redisClient, err = redis.Dial("tcp", redisURL, redis.DialDatabase(redisDatabase), redis.DialPassword(redisPassword))
		if err != nil {
			logger.Log("msg", "could not create redis client", "error", err)
			return
		}
		defer redisClient.Close()

		// Create logger that duplicates logs to stdout and Redis.
		logger = log.NewJSONLogger(io.MultiWriter(os.Stdout, elasticsearch_bridge.NewLogstashRedisWriter(redisClient, ComponentName)))
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}

	// Flaki.
	var flakiClient fb_flaki.FlakiClient
	{
		// Set up a connection to the flaki-service.
		var conn *grpc.ClientConn
		{
			var err error
			conn, err = grpc.Dial(flakiAddr, grpc.WithInsecure(), grpc.WithCodec(flatbuffers.FlatbuffersCodec{}))
			if err != nil {
				logger.Log("msg", "could not connect to flaki-service", "error", err)
				return
			}
			defer conn.Close()
		}

		flakiClient = fb_flaki.NewFlakiClient(conn)
	}

	// Get unique ID for this component
	var ComponentID string
	{
		var b = flatbuffers.NewBuilder(0)
		fb_flaki.FlakiRequestStart(b)
		b.Finish(fb_flaki.FlakiRequestEnd(b))

		var reply, err = flakiClient.NextValidID(context.Background(), b)

		if err != nil {
			logger.Log("msg", "cannot get ID from flaki-service", "error", err)
			return
		}

		ComponentID = string(reply.Id())
	}

	// Add component name, component ID and version to the logger tags.
	logger = log.With(logger, "component_name", ComponentName, "component_id", ComponentID, "component_version", Version)

	// Log component version infos.
	logger.Log("environment", Environment, "git_commit", GitCommit)

	// Critical errors channel.
	var errc = make(chan error)
	go func() {
		var c = make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()

	// Sentry.
	type Sentry interface {
		CaptureError(err error, tags map[string]string, interfaces ...sentry.Interface) string
		URL() string
		Close()
	}

	var sentryClient Sentry = &elasticsearch_bridge.NoopSentry{}
	if sentryEnabled {
		var logger = log.With(logger, "unit", "sentry")
		var err error
		sentryClient, err = sentry.New(sentryDSN)
		if err != nil {
			logger.Log("msg", "could not create Sentry client", "error", err)
			return
		}
		defer sentryClient.Close()
	}

	// Influx client.
	type Metrics interface {
		NewCounter(name string) metrics.Counter
		NewGauge(name string) metrics.Gauge
		NewHistogram(name string) metrics.Histogram
		WriteLoop(c <-chan time.Time)
		Ping(timeout time.Duration) (time.Duration, string, error)
	}

	var influxMetrics Metrics = &elasticsearch_bridge.NoopMetrics{}
	if influxEnabled {
		var logger = log.With(logger, "unit", "influx")

		var influxClient, err = influx.NewHTTPClient(influxHTTPConfig)
		if err != nil {
			logger.Log("msg", "could not create Influx client", "error", err)
			return
		}
		defer influxClient.Close()

		var gokitInflux = gokit_influx.New(
			map[string]string{},
			influxBatchPointsConfig,
			log.With(logger, "unit", "go-kit influx"),
		)

		influxMetrics = elasticsearch_bridge.NewMetrics(influxClient, gokitInflux)
	}

	// Jaeger client.
	var tracer opentracing.Tracer
	{
		var logger = log.With(logger, "unit", "jaeger")
		var closer io.Closer
		var err error

		tracer, closer, err = jaegerConfig.New(ComponentName)
		if err != nil {
			logger.Log("msg", "could not create Jaeger tracer", "error", err)
			return
		}
		defer closer.Close()
	}

	// Systemd D-Bus connection.
	var systemDConn *dbus.Conn
	{
		var err error
		systemDConn, err = dbus.New()
		if err != nil {
			logger.Log("msg", "could not create systemd D-Bus connection", "error", err)
			return
		}
	}

	// Elasticsearch Client
	var elasticsearchClient *elasticsearch_bridge.Client
	{
		var err error
		var logger = log.With(logger, "unit", "elasticsearch")

		elasticsearchClient, err = elasticsearch_bridge.New(elasticsearchConfig)
		if err != nil {
			logger.Log("msg", "could not create elasticsearch client", "error", err)
			return
		}
	}

	// Cockroach DB.
	type Cockroach interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
		Query(query string, args ...interface{}) (*sql.Rows, error)
		QueryRow(query string, args ...interface{}) *sql.Row
	}

	var cHealthDB Cockroach
	var cJobsDB Cockroach
	if cockroachEnabled {
		var err error
		cHealthDB, err = sql.Open("postgres", fmt.Sprintf("postgresql://%s:%s@%s/%s?sslmode=disable", cockroachUsername, cockroachPassword, cockroachHostPort, cockroachHealthDB))
		if err != nil {
			logger.Log("msg", "could not create cockroach DB connection for health DB", "error", err)
			return
		}
		cJobsDB, err = sql.Open("postgres", fmt.Sprintf("postgresql://%s:%s@%s/%s?sslmode=disable", cockroachUsername, cockroachPassword, cockroachHostPort, cockroachJobsDB))
		if err != nil {
			logger.Log("msg", "could not create cockroach DB connection for health DB", "error", err)
			return
		}
	}

	// Health service.
	var healthLogger = log.With(logger, "svc", "health")

	var cockroachModule *health.StorageModule
	{
		cockroachModule = health.NewStorageModule(ComponentName, ComponentID, cHealthDB)
	}

	var influxHM health.InfluxHealthChecker
	{
		influxHM = common.NewInfluxModule(influxMetrics, influxEnabled)
		influxHM = common.MakeInfluxModuleLoggingMW(log.With(healthLogger, "mw", "module"))(influxHM)
	}
	var jaegerHM health.JaegerHealthChecker
	{
		jaegerHM = common.NewJaegerModule(systemDConn, http.DefaultClient, jaegerCollectorHealthcheckURL, jaegerEnabled)
		jaegerHM = common.MakeJaegerModuleLoggingMW(log.With(healthLogger, "mw", "module"))(jaegerHM)
	}
	var redisHM health.RedisHealthChecker
	{
		redisHM = common.NewRedisModule(redisClient, redisEnabled)
		redisHM = common.MakeRedisModuleLoggingMW(log.With(healthLogger, "mw", "module"))(redisHM)
	}
	var sentryHM health.SentryHealthChecker
	{
		sentryHM = common.NewSentryModule(sentryClient, http.DefaultClient, sentryEnabled)
		sentryHM = common.MakeSentryModuleLoggingMW(log.With(healthLogger, "mw", "module"))(sentryHM)
	}
	var flakiHM health.FlakiHealthChecker
	{
		flakiHM = common.NewFlakiModule(elasticsearch_bridge.NewFlakiLightClient(flakiClient))
		flakiHM = common.MakeFlakiModuleLoggingMW(log.With(healthLogger, "mw", "module"))(flakiHM)
	}
	var elasticsearchHM health.ElasticsearchHealthChecker
	{
		elasticsearchHM = health.NewElasticsearchModule(elasticsearchClient)
		elasticsearchHM = health.MakeElasticsearchModuleLoggingMW(log.With(healthLogger, "mw", "module"))(elasticsearchHM)
	}
	var healthComponent health.HealthChecker
	{
		healthComponent = health.NewComponent(influxHM, jaegerHM, redisHM, sentryHM, flakiHM, elasticsearchHM, cockroachModule, healthChecksValidity)
		healthComponent = health.MakeComponentLoggingMW(log.With(healthLogger, "mw", "component"))(healthComponent)
	}

	var influxExecHealthEndpoint endpoint.Endpoint
	{
		influxExecHealthEndpoint = health.MakeExecInfluxHealthCheckEndpoint(healthComponent)
		influxExecHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ExecInfluxHealthCheck"))(influxExecHealthEndpoint)
		influxExecHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(influxExecHealthEndpoint)
	}
	var influxReadHealthEndpoint endpoint.Endpoint
	{
		influxReadHealthEndpoint = health.MakeReadInfluxHealthCheckEndpoint(healthComponent)
		influxReadHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ReadInfluxHealthCheck"))(influxReadHealthEndpoint)
		influxReadHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(influxReadHealthEndpoint)
	}
	var jaegerExecHealthEndpoint endpoint.Endpoint
	{
		jaegerExecHealthEndpoint = health.MakeExecJaegerHealthCheckEndpoint(healthComponent)
		jaegerExecHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ExecJaegerHealthCheck"))(jaegerExecHealthEndpoint)
		jaegerExecHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(jaegerExecHealthEndpoint)
	}
	var jaegerReadHealthEndpoint endpoint.Endpoint
	{
		jaegerReadHealthEndpoint = health.MakeReadJaegerHealthCheckEndpoint(healthComponent)
		jaegerReadHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ReadJaegerHealthCheck"))(jaegerReadHealthEndpoint)
		jaegerReadHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(jaegerReadHealthEndpoint)
	}
	var redisExecHealthEndpoint endpoint.Endpoint
	{
		redisExecHealthEndpoint = health.MakeExecRedisHealthCheckEndpoint(healthComponent)
		redisExecHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ExecRedisHealthCheck"))(redisExecHealthEndpoint)
		redisExecHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(redisExecHealthEndpoint)
	}
	var redisReadHealthEndpoint endpoint.Endpoint
	{
		redisReadHealthEndpoint = health.MakeReadRedisHealthCheckEndpoint(healthComponent)
		redisReadHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ReadRedisHealthCheck"))(redisReadHealthEndpoint)
		redisReadHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(redisReadHealthEndpoint)
	}
	var sentryExecHealthEndpoint endpoint.Endpoint
	{
		sentryExecHealthEndpoint = health.MakeExecSentryHealthCheckEndpoint(healthComponent)
		sentryExecHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ExecSentryHealthCheck"))(sentryExecHealthEndpoint)
		sentryExecHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(sentryExecHealthEndpoint)
	}
	var sentryReadHealthEndpoint endpoint.Endpoint
	{
		sentryReadHealthEndpoint = health.MakeReadSentryHealthCheckEndpoint(healthComponent)
		sentryReadHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ReadSentryHealthCheck"))(sentryReadHealthEndpoint)
		sentryReadHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(sentryReadHealthEndpoint)
	}
	var flakiExecHealthEndpoint endpoint.Endpoint
	{
		flakiExecHealthEndpoint = health.MakeExecFlakiHealthCheckEndpoint(healthComponent)
		flakiExecHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ExecFlakiHealthCheck"))(flakiExecHealthEndpoint)
		flakiExecHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(flakiExecHealthEndpoint)
	}
	var flakiReadHealthEndpoint endpoint.Endpoint
	{
		flakiReadHealthEndpoint = health.MakeReadFlakiHealthCheckEndpoint(healthComponent)
		flakiReadHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ReadFlakiHealthCheck"))(flakiReadHealthEndpoint)
		flakiReadHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(flakiReadHealthEndpoint)
	}
	var elasticsearchExecHealthEndpoint endpoint.Endpoint
	{
		elasticsearchExecHealthEndpoint = health.MakeExecElasticsearchHealthCheckEndpoint(healthComponent)
		elasticsearchExecHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ExecElasticsearchHealthCheck"))(elasticsearchExecHealthEndpoint)
		elasticsearchExecHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(elasticsearchExecHealthEndpoint)
	}
	var elasticsearchReadHealthEndpoint endpoint.Endpoint
	{
		elasticsearchReadHealthEndpoint = health.MakeReadElasticsearchHealthCheckEndpoint(healthComponent)
		elasticsearchReadHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "ReadElasticsearchHealthCheck"))(elasticsearchReadHealthEndpoint)
		elasticsearchReadHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(elasticsearchReadHealthEndpoint)
	}
	var allHealthEndpoint endpoint.Endpoint
	{
		allHealthEndpoint = health.MakeAllHealthChecksEndpoint(healthComponent)
		allHealthEndpoint = health.MakeEndpointLoggingMW(log.With(healthLogger, "mw", "endpoint", "unit", "AllHealthCheck"))(allHealthEndpoint)
		allHealthEndpoint = health.MakeEndpointCorrelationIDMW(flakiClient, tracer)(allHealthEndpoint)
	}

	var healthEndpoints = health.Endpoints{
		InfluxExecHealthCheck:        influxExecHealthEndpoint,
		InfluxReadHealthCheck:        influxReadHealthEndpoint,
		JaegerExecHealthCheck:        jaegerExecHealthEndpoint,
		JaegerReadHealthCheck:        jaegerReadHealthEndpoint,
		RedisExecHealthCheck:         redisExecHealthEndpoint,
		RedisReadHealthCheck:         redisReadHealthEndpoint,
		SentryExecHealthCheck:        sentryExecHealthEndpoint,
		SentryReadHealthCheck:        sentryReadHealthEndpoint,
		FlakiExecHealthCheck:         flakiExecHealthEndpoint,
		FlakiReadHealthCheck:         flakiReadHealthEndpoint,
		ElasticsearchExecHealthCheck: elasticsearchExecHealthEndpoint,
		ElasticsearchReadHealthCheck: elasticsearchReadHealthEndpoint,
		AllHealthChecks:              allHealthEndpoint,
	}

	// Local Jobs
	{
		var localCtrl = controller.NewController(ComponentName, ComponentID, &idGenerator{flakiClient}, &job_lock.NoopLocker{}, controller.EnableStatusStorage(job_status.New(cJobsDB)))

		var influxJob *job.Job
		{
			var err error
			influxJob, err = health_job.MakeInfluxJob(influxHM, healthChecksValidity[influxKey], cockroachModule)
			if err != nil {
				logger.Log("msg", "could not create influx health job", "error", err)
				return
			}
			localCtrl.Register(influxJob)
			localCtrl.Schedule("@minutely", influxJob.Name())
		}

		var jaegerJob *job.Job
		{
			var err error
			jaegerJob, err = health_job.MakeJaegerJob(jaegerHM, healthChecksValidity[jaegerKey], cockroachModule)
			if err != nil {
				logger.Log("msg", "could not create jaeger health job", "error", err)
				return
			}
			localCtrl.Register(jaegerJob)
			localCtrl.Schedule("@minutely", jaegerJob.Name())
		}

		var redisJob *job.Job
		{
			var err error
			redisJob, err = health_job.MakeRedisJob(redisHM, healthChecksValidity[redisKey], cockroachModule)
			if err != nil {
				logger.Log("msg", "could not create redis health job", "error", err)
				return
			}
			localCtrl.Register(redisJob)
			localCtrl.Schedule("@minutely", redisJob.Name())
		}

		var sentryJob *job.Job
		{
			var err error
			sentryJob, err = health_job.MakeSentryJob(sentryHM, healthChecksValidity[sentryKey], cockroachModule)
			if err != nil {
				logger.Log("msg", "could not create sentry health job", "error", err)
				return
			}
			localCtrl.Register(sentryJob)
			localCtrl.Schedule("@minutely", sentryJob.Name())
		}

		var flakiJob *job.Job
		{
			var err error
			flakiJob, err = health_job.MakeFlakiJob(flakiHM, healthChecksValidity[flakiKey], cockroachModule)
			if err != nil {
				logger.Log("msg", "could not create flaki health job", "error", err)
				return
			}
			localCtrl.Register(flakiJob)
			localCtrl.Schedule("@minutely", flakiJob.Name())
		}

		var elasticsearchJob *job.Job
		{
			var err error
			elasticsearchJob, err = health_job.MakeElasticsearchJob(elasticsearchHM, healthChecksValidity[elasticsearchKey], cockroachModule)
			if err != nil {
				logger.Log("msg", "could not create elasticsearch health job", "error", err)
				return
			}
			localCtrl.Register(elasticsearchJob)
			localCtrl.Schedule("@minutely", elasticsearchJob.Name())
		}

		var cleanHealthChecksJob *job.Job
		{
			var err error
			cleanHealthChecksJob, err = health_job.MakeCleanCockroachJob(cockroachModule, log.With(logger, "job", "clean health checks"))
			if err != nil {
				logger.Log("msg", "could not create clean health checks job", "error", err)
				return
			}
			localCtrl.Register(cleanHealthChecksJob)
			localCtrl.Schedule(fmt.Sprintf("@every %s", cockroachCleanInterval), cleanHealthChecksJob.Name())

		}

		localCtrl.Start()
	}

	// Distributed Jobs
	{
		var distributedCtrl = controller.NewController(ComponentName, ComponentID, &idGenerator{flakiClient}, job_lock.New(cJobsDB), controller.EnableStatusStorage(job_status.New(cJobsDB)))

		var cleanElasticIndexesJob *job.Job
		{
			var err error
			cleanElasticIndexesJob, err = health_job.MakeElasticsearchCleanIndexJob(elasticsearchClient, elasticsearchIndexExpiration)
			if err != nil {
				logger.Log("msg", "could not create clean elastic indexes job", "error", err)
				return
			}
			distributedCtrl.Register(cleanElasticIndexesJob)
			distributedCtrl.Schedule(fmt.Sprintf("@every %s", elasticsearchIndexCleanInterval), cleanElasticIndexesJob.Name())
		}

		distributedCtrl.Start()
	}

	// HTTP server.
	go func() {
		var logger = log.With(logger, "transport", "http")
		logger.Log("addr", httpAddr)

		var route = mux.NewRouter()

		// Version.
		route.Handle("/", http.HandlerFunc(makeVersion(ComponentName, ComponentID, Version, Environment, GitCommit)))

		// Health checks.
		var healthSubroute = route.PathPrefix("/health").Subrouter()

		var allHealthChecksHandler = health.MakeHealthCheckHandler(healthEndpoints.AllHealthChecks)
		healthSubroute.Handle("", allHealthChecksHandler)

		healthSubroute.Handle("/influx", health.MakeHealthCheckHandler(healthEndpoints.InfluxReadHealthCheck)).Methods("GET")
		healthSubroute.Handle("/influx", health.MakeHealthCheckHandler(healthEndpoints.InfluxExecHealthCheck)).Methods("POST")

		healthSubroute.Handle("/jaeger", health.MakeHealthCheckHandler(healthEndpoints.JaegerReadHealthCheck)).Methods("GET")
		healthSubroute.Handle("/jaeger", health.MakeHealthCheckHandler(healthEndpoints.JaegerExecHealthCheck)).Methods("POST")

		healthSubroute.Handle("/redis", health.MakeHealthCheckHandler(healthEndpoints.RedisReadHealthCheck)).Methods("GET")
		healthSubroute.Handle("/redis", health.MakeHealthCheckHandler(healthEndpoints.RedisExecHealthCheck)).Methods("POST")

		healthSubroute.Handle("/sentry", health.MakeHealthCheckHandler(healthEndpoints.SentryReadHealthCheck)).Methods("GET")
		healthSubroute.Handle("/sentry", health.MakeHealthCheckHandler(healthEndpoints.SentryExecHealthCheck)).Methods("POST")

		healthSubroute.Handle("/flaki", health.MakeHealthCheckHandler(healthEndpoints.FlakiReadHealthCheck)).Methods("GET")
		healthSubroute.Handle("/flaki", health.MakeHealthCheckHandler(healthEndpoints.FlakiExecHealthCheck)).Methods("POST")

		healthSubroute.Handle("/elasticsearch", health.MakeHealthCheckHandler(healthEndpoints.ElasticsearchReadHealthCheck)).Methods("GET")
		healthSubroute.Handle("/elasticsearch", health.MakeHealthCheckHandler(healthEndpoints.ElasticsearchExecHealthCheck)).Methods("POST")

		// Debug.
		if pprofRouteEnabled {
			var debugSubroute = route.PathPrefix("/debug").Subrouter()
			debugSubroute.HandleFunc("/pprof/", http.HandlerFunc(pprof.Index))
			debugSubroute.HandleFunc("/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
			debugSubroute.HandleFunc("/pprof/profile", http.HandlerFunc(pprof.Profile))
			debugSubroute.HandleFunc("/pprof/symbol", http.HandlerFunc(pprof.Symbol))
			debugSubroute.HandleFunc("/pprof/trace", http.HandlerFunc(pprof.Trace))
		}

		errc <- http.ListenAndServe(httpAddr, route)
	}()

	// Influx writing.
	go func() {
		var tic = time.NewTicker(influxWriteInterval)
		defer tic.Stop()
		influxMetrics.WriteLoop(tic.C)
	}()

	// Redis writing.
	if redisEnabled {
		go func() {
			var tic = time.NewTicker(redisWriteInterval)
			defer tic.Stop()
			for range tic.C {
				redisClient.Flush()
			}
		}()
	}
	logger.Log("error", <-errc)
}

type idGenerator struct {
	flaki fb_flaki.FlakiClient
}

func (g *idGenerator) NextID() string {
	var b = flatbuffers.NewBuilder(0)
	fb_flaki.FlakiRequestStart(b)
	b.Finish(fb_flaki.FlakiRequestEnd(b))

	var reply, err = g.flaki.NextValidID(context.Background(), b)

	// If we cannot get ID from Flaki, we generate a random one.
	if err != nil {
		rand.Seed(time.Now().UnixNano())
		return "degraded-" + strconv.FormatUint(rand.Uint64(), 10)
	}

	return string(reply.Id())
}

type info struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Version string `json:"version"`
	Env     string `json:"environment"`
	Commit  string `json:"commit"`
}

// makeVersion makes a HTTP handler that returns information about the version of the service.
func makeVersion(componentName, componentID, version, environment, gitCommit string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		var infos = info{
			Name:    componentName,
			ID:      componentID,
			Version: version,
			Env:     environment,
			Commit:  gitCommit,
		}

		var j, err = json.MarshalIndent(infos, "", "  ")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write(j)
		}
	}
}

func config(logger log.Logger) *viper.Viper {
	logger.Log("msg", "load configuration and command args")

	var v = viper.New()

	// Component default.
	v.SetDefault("config-file", "./configs/elasticsearch_bridge.yml")
	v.SetDefault("component-http-host-port", "0.0.0.0:8888")

	// ElasticSearch
	v.SetDefault("elasticsearch-host-port", "")
	v.SetDefault("elasticsearch-index-clean-interval", "24h")
	v.SetDefault("elasticsearch-index-expiration", "24h")

	// Flaki
	v.SetDefault("flaki-host-port", "")

	// Influx DB client default.
	v.SetDefault("influx", false)
	v.SetDefault("influx-host-port", "")
	v.SetDefault("influx-username", "")
	v.SetDefault("influx-password", "")
	v.SetDefault("influx-database", "")
	v.SetDefault("influx-precision", "")
	v.SetDefault("influx-retention-policy", "")
	v.SetDefault("influx-write-consistency", "")
	v.SetDefault("influx-write-interval", 1000)

	// Sentry client default.
	v.SetDefault("sentry", false)
	v.SetDefault("sentry-dsn", "")

	// Jaeger tracing default.
	v.SetDefault("jaeger", false)
	v.SetDefault("jaeger-sampler-type", "")
	v.SetDefault("jaeger-sampler-param", 0)
	v.SetDefault("jaeger-sampler-host-port", "")
	v.SetDefault("jaeger-reporter-logspan", false)
	v.SetDefault("jaeger-write-interval", "1s")
	v.SetDefault("jaeger-collector-healthcheck-host-port", "")

	// Debug routes enabled.
	v.SetDefault("pprof-route-enabled", true)

	// Redis.
	v.SetDefault("redis", false)
	v.SetDefault("redis-host-port", "")
	v.SetDefault("redis-password", "")
	v.SetDefault("redis-database", 0)
	v.SetDefault("redis-database", 0)
	v.SetDefault("redis-write-interval", "1s")

	// Cockroach.
	v.SetDefault("cockroach", false)
	v.SetDefault("cockroach-host-port", "")
	v.SetDefault("cockroach-username", "")
	v.SetDefault("cockroach-password", "")
	v.SetDefault("cockroach-health-database", "")
	v.SetDefault("cockroach-jobs-database", "")
	v.SetDefault("cockroach-clean-interval", "24h")

	// Jobs
	v.SetDefault("job-influx-health-validity", "1m")
	v.SetDefault("job-jaeger-health-validity", "1m")
	v.SetDefault("job-redis-health-validity", "1m")
	v.SetDefault("job-sentry-health-validity", "1m")

	// First level of override.
	pflag.String("config-file", v.GetString("config-file"), "The configuration file path can be relative or absolute.")
	v.BindPFlag("config-file", pflag.Lookup("config-file"))
	pflag.Parse()

	// Load config.
	v.SetConfigFile(v.GetString("config-file"))
	var err = v.ReadInConfig()
	if err != nil {
		logger.Log("error", err)
	}

	// If the host/port is not set, we consider the components deactivated.
	v.Set("influx", v.GetString("influx-host-port") != "")
	v.Set("sentry", v.GetString("sentry-dsn") != "")
	v.Set("jaeger", v.GetString("jaeger-sampler-host-port") != "")
	v.Set("redis", v.GetString("redis-host-port") != "")
	v.Set("cockroach", v.GetString("cockroach-host-port") != "")

	// Log config in alphabetical order.
	var keys = v.AllKeys()
	sort.Strings(keys)

	for _, k := range keys {
		logger.Log(k, v.Get(k))
	}

	return v
}
