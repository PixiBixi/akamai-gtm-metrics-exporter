package collectors

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testOpts = PollOptions{Interval: time.Hour, CacheTTL: time.Hour}

func dcRow(timestamp string, requests ...int64) *DatacenterTrafficData {
	row := &DatacenterTrafficData{Timestamp: timestamp}
	for i, r := range requests {
		row.Properties = append(row.Properties, &TrafficProperty{Name: fmt.Sprintf("prop%d", i), Requests: r})
	}
	return row
}

func dcConfig(domain string, ids ...int) GTMMetricsConfig {
	d := &DomainTraffic{Name: domain}
	for _, id := range ids {
		d.Datacenters = append(d.Datacenters, &TrafficDatacenterConfig{DatacenterID: id})
	}
	return GTMMetricsConfig{Domains: []*DomainTraffic{d}, UseTimestamp: boolPtr(false)}
}

func TestTrafficQueryRange(t *testing.T) {
	now := ts("2026-10-02T20:00:00Z")
	wStart, wEnd := ts("2026-07-04T20:00:00Z"), ts("2026-10-02T19:30:00Z")
	tests := []struct {
		name       string
		last, wEnd time.Time
		start, end time.Time
		ok         bool
	}{
		{"behind the window end", ts("2026-10-02T19:00:00Z"), wEnd, ts("2026-10-02T19:00:00Z"), wEnd, true},
		{"start aligned on a 5 minute bucket", ts("2026-10-02T19:23:00Z"), wEnd, ts("2026-10-02T19:20:00Z"), wEnd, true},
		{"caught up with the window", ts("2026-10-02T19:30:00Z"), wEnd, wEnd, wEnd, false},
		{"exporter started after the window end", ts("2026-10-02T19:50:00Z"), wEnd, wEnd, wEnd, false},
		{"older than the window retention", ts("2026-01-01T00:00:00Z"), wEnd, wStart, wEnd, true},
		{"next interval not due yet", now.Add(-3 * time.Minute), now.Add(time.Hour), now, now, false},
		{"never a partial bucket up to now", ts("2026-10-02T19:58:00Z"), now.Add(time.Hour), now, now, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok := trafficQueryRange(tt.last, wStart, tt.wEnd, now)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.start, start)
			assert.Equal(t, tt.end, end)
		})
	}
}

// A target without traffic must not query an ever growing range.
func TestDatacenterCollectorEmptyReportAdvancesCursor(t *testing.T) {
	f := newFakeAPI(t)
	f.trafficWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:30:00Z")}
	f.dcRows["example.akadns.net/3131"] = []*DatacenterTrafficData{}
	c := NewDatacenterTrafficCollector(f.pool(t, 1), prometheus.NewRegistry(), dcConfig("example.akadns.net", 3131),
		"akamai_gtm_", ts("2026-10-02T18:03:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))
	assert.Equal(t, ts("2026-10-02T19:29:59Z"), c.targets[0].last)

	f.mu.Lock()
	f.trafficWindow[1] = ts("2026-10-02T19:35:00Z")
	f.mu.Unlock()
	start, end, ok := trafficQueryRange(c.targets[0].last, f.trafficWindow[0], f.trafficWindow[1], ts("2026-10-02T20:10:00Z"))
	assert.True(t, ok)
	assert.Equal(t, ts("2026-10-02T19:30:00Z"), start)
	assert.Equal(t, ts("2026-10-02T19:35:00Z"), end)
}

func TestDatacenterCollectorScrapeNeverCallsAPI(t *testing.T) {
	f := newFakeAPI(t)
	f.trafficWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:30:00Z")}
	f.dcRows["example.akadns.net/3131"] = []*DatacenterTrafficData{dcRow("2026-10-02T19:25:00Z", 10, 5)}

	c := NewDatacenterTrafficCollector(f.pool(t, 2), prometheus.NewRegistry(), dcConfig("example.akadns.net", 3131),
		"akamai_gtm_", ts("2026-10-02T19:20:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))
	calls := f.callCount("dc-report") + f.callCount("dc-window")

	for i := 0; i < 5; i++ {
		got := collectSamples(t, c)
		assert.Equal(t, 15.0, got[`akamai_gtm_datacenter_traffic_requests_per_interval{datacenter="3131",domain="example.akadns.net"}`].value)
	}
	assert.Equal(t, calls, f.callCount("dc-report")+f.callCount("dc-window"), "Collect must not call the API")
}

func TestDatacenterCollectorWindowFetchedOncePerCycle(t *testing.T) {
	f := newFakeAPI(t)
	f.trafficWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:30:00Z")}
	ids := []int{3131, 3132, 3133, 3134, 3135}
	for _, id := range ids {
		f.dcRows[fmt.Sprintf("example.akadns.net/%d", id)] = []*DatacenterTrafficData{dcRow("2026-10-02T19:25:00Z", int64(id))}
	}
	c := NewDatacenterTrafficCollector(f.pool(t, 3), prometheus.NewRegistry(), dcConfig("example.akadns.net", ids...),
		"akamai_gtm_", ts("2026-10-02T19:20:00Z"), time.Hour, testOpts)

	require.NoError(t, c.poll(context.Background()))
	assert.Equal(t, 1, f.callCount("dc-window"))
	assert.Equal(t, len(ids), f.callCount("dc-report"))

	// Nothing new in the window: only the window is fetched.
	require.NoError(t, c.poll(context.Background()))
	assert.Equal(t, 2, f.callCount("dc-window"))
	assert.Equal(t, len(ids), f.callCount("dc-report"))
}

func TestDatacenterCollectorExposesOneRowPerCycleWithoutRefetching(t *testing.T) {
	f := newFakeAPI(t)
	f.trafficWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:30:00Z")}
	f.dcRows["example.akadns.net/3131"] = []*DatacenterTrafficData{
		dcRow("2026-10-02T19:30:00Z", 3), dcRow("2026-10-02T19:20:00Z", 1), dcRow("2026-10-02T19:25:00Z", 2),
	}
	cfg := dcConfig("example.akadns.net", 3131)
	cfg.UseTimestamp = nil // default: samples carry the report timestamp
	c := NewDatacenterTrafficCollector(f.pool(t, 1), prometheus.NewRegistry(), cfg,
		"akamai_gtm_", ts("2026-10-02T19:15:00Z"), time.Hour, testOpts)
	key := `akamai_gtm_datacenter_traffic_requests_per_interval{datacenter="3131",domain="example.akadns.net"}`

	for i, want := range []struct {
		value float64
		ts    string
	}{{1, "2026-10-02T19:20:00Z"}, {2, "2026-10-02T19:25:00Z"}, {3, "2026-10-02T19:30:00Z"}} {
		require.NoError(t, c.poll(context.Background()))
		got := collectSamples(t, c)[key]
		assert.Equal(t, want.value, got.value, "cycle %d", i)
		assert.Equal(t, ts(want.ts).UnixMilli(), got.timestamp, "cycle %d", i)
	}
	assert.Equal(t, 1, f.callCount("dc-report"), "queued rows must not be refetched")
}

func TestDatacenterCollectorPropertyFilter(t *testing.T) {
	f := newFakeAPI(t)
	f.trafficWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:30:00Z")}
	f.dcRows["example.akadns.net/3131"] = []*DatacenterTrafficData{dcRow("2026-10-02T19:25:00Z", 10, 5)}
	cfg := dcConfig("example.akadns.net", 3131)
	cfg.Domains[0].Datacenters[0].Properties = []string{"prop1"}
	reg := prometheus.NewRegistry()
	c := NewDatacenterTrafficCollector(f.pool(t, 1), reg, cfg, "akamai_gtm_", ts("2026-10-02T19:20:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))

	got := collectSamples(t, c)
	assert.Len(t, got, 1)
	assert.Equal(t, 5.0, got[`akamai_gtm_datacenter_traffic_requests_per_interval{datacenter="3131",domain="example.akadns.net",property="prop1"}`].value)

	// The summary still observes the aggregate of every property.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 1)
	assert.Equal(t, 15.0, mfs[0].GetMetric()[0].GetSummary().GetSampleSum())
}

func TestPropertyCollectorFilters(t *testing.T) {
	f := newFakeAPI(t)
	f.trafficWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:30:00Z")}
	f.propRows["example.akadns.net/www"] = []*PropertyTrafficData{{
		Timestamp: "2026-10-02T19:25:00Z",
		Datacenters: []*PropertyDCData{
			{DatacenterId: 3131, Nickname: "EUW1", TrafficTargetName: "target-a", Requests: 7},
			{DatacenterId: 3132, Nickname: "USE1", TrafficTargetName: "target-b", Requests: 11},
			{DatacenterId: 3133, Nickname: "APAC", TrafficTargetName: "target-c", Requests: 13},
		},
	}}
	f.propRows["example.akadns.net/api"] = f.propRows["example.akadns.net/www"]
	cfg := GTMMetricsConfig{UseTimestamp: boolPtr(false), Domains: []*DomainTraffic{{
		Name: "example.akadns.net",
		Properties: []*TrafficPropertyConfig{
			{Name: "www", DatacenterIDs: []int{3131}, DCNicknames: []string{"USE1"}, Targets: []string{"target-c"}},
			{Name: "api"},
		},
	}}}
	c := NewPropertyTrafficCollector(f.pool(t, 2), prometheus.NewRegistry(), cfg, "akamai_gtm_", ts("2026-10-02T19:20:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))

	assert.Equal(t, map[string]sample{
		`akamai_gtm_property_traffic_requests_per_interval{datacenterid="3131",domain="example.akadns.net",property="www"}`: {value: 7},
		`akamai_gtm_property_traffic_requests_per_interval{domain="example.akadns.net",nickname="USE1",property="www"}`:     {value: 11},
		`akamai_gtm_property_traffic_requests_per_interval{domain="example.akadns.net",property="www",target="target-c"}`:   {value: 13},
		`akamai_gtm_property_traffic_requests_per_interval{domain="example.akadns.net",property="api"}`:                     {value: 31},
	}, collectSamples(t, c))
	assert.Equal(t, 1, f.callCount("prop-window"))
}

func livenessConfig(domain string, props ...string) GTMMetricsConfig {
	d := &DomainTraffic{Name: domain}
	for _, p := range props {
		d.Liveness = append(d.Liveness, &LivenessTestConfig{PropertyName: p, ErrorCode: true})
	}
	return GTMMetricsConfig{Domains: []*DomainTraffic{d}, UseTimestamp: boolPtr(false)}
}

func liveRow(timestamp string, dcID int, errorCode, duration int64) *LivenessTData {
	return &LivenessTData{Timestamp: timestamp, Datacenters: []*LivenessDRow{{
		DatacenterID: dcID, ErrorCode: errorCode, Duration: duration, AgentIP: "23.215.50.4", TargetIP: "216.22.16.32",
	}}}
}

func TestLivenessCollectorMetrics(t *testing.T) {
	f := newFakeAPI(t)
	f.livenessWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:44:00Z")}
	f.liveRows["example.akadns.net/www"] = []*LivenessTData{liveRow("2026-10-02T19:42:34Z", 3138, 2503, 60)}
	reg := prometheus.NewRegistry()
	c := NewLivenessTrafficCollector(f.pool(t, 1), reg, livenessConfig("example.akadns.net", "www"), "akamai_gtm_", ts("2026-10-02T19:40:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))

	labels := `{datacenter="3138",domain="example.akadns.net",errorcode="2503",property="www"}`
	assert.Equal(t, map[string]sample{
		"akamai_gtm_property_liveness_errors_datacenter_failures" + labels:         {value: 1},
		"akamai_gtm_property_liveness_errors_datacenter_failure_duration" + labels: {value: 60},
	}, collectSamples(t, c))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]uint64{}
	for _, mf := range mfs {
		m := mf.GetMetric()[0]
		names[mf.GetName()] = m.GetHistogram().GetSampleCount() + m.GetSummary().GetSampleCount()
	}
	assert.Equal(t, map[string]uint64{
		"akamai_gtm_property_liveness_errors_duration_per_datacenter_histogram": 1,
		"akamai_gtm_property_liveness_errors_errors_per_datacenter_summary":     1,
	}, names)
}

// The scrape-time collector stayed on the previous day forever once that day
// had any error row, so errors after midnight UTC were never reported.
func TestLivenessCollectorCrossesMidnight(t *testing.T) {
	f := newFakeAPI(t)
	f.livenessWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-01T23:50:00Z")}
	f.liveRows["example.akadns.net/www"] = []*LivenessTData{liveRow("2026-10-01T23:45:00Z", 3138, 2503, 60)}
	c := NewLivenessTrafficCollector(f.pool(t, 1), prometheus.NewRegistry(), livenessConfig("example.akadns.net", "www"), "akamai_gtm_", ts("2026-10-01T23:40:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))

	f.mu.Lock()
	f.livenessWindow[1] = ts("2026-10-02T00:12:00Z")
	f.liveRows["example.akadns.net/www"] = append(f.liveRows["example.akadns.net/www"], liveRow("2026-10-02T00:10:00Z", 3138, 2504, 120))
	f.mu.Unlock()
	require.NoError(t, c.poll(context.Background()))

	got := collectSamples(t, c)
	assert.Equal(t, 120.0, got[`akamai_gtm_property_liveness_errors_datacenter_failure_duration{datacenter="3138",domain="example.akadns.net",errorcode="2504",property="www"}`].value)
}

func TestLivenessCollectorFilterQuery(t *testing.T) {
	f := newFakeAPI(t)
	f.livenessWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:44:00Z")}
	f.liveRows["example.akadns.net/www"] = []*LivenessTData{liveRow("2026-10-02T19:42:34Z", 3138, 2503, 60)}
	cfg := livenessConfig("example.akadns.net", "www")
	cfg.Domains[0].Liveness[0].TargetIP = "216.22.16.32"
	c := NewLivenessTrafficCollector(f.pool(t, 1), prometheus.NewRegistry(), cfg, "akamai_gtm_", ts("2026-10-02T19:40:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))

	assert.Contains(t, collectSamples(t, c),
		`akamai_gtm_property_liveness_errors_datacenter_failures{datacenter="3138",domain="example.akadns.net",errorcode="2503",property="www",targetip="216.22.16.32"}`)
}

func burstRows(from string, n int) []*LivenessTData {
	start := ts(from)
	var rows []*LivenessTData
	for i := 0; i < n; i++ {
		row := liveRow(start.Add(time.Duration(i)*10*time.Second).Format(GTMTrafficLongTimeFormat), 3135, 2503, 60)
		row.Datacenters[0].AgentIP = fmt.Sprintf("23.0.0.%d", i) // one row per test agent
		rows = append(rows, row)
	}
	return rows
}

// A burst (one row per test agent) is exposed in one cycle, not one row per cycle.
func TestLivenessCollectorBurstInOneCycle(t *testing.T) {
	f := newFakeAPI(t)
	f.livenessWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T21:18:00Z")}
	f.liveRows["example.akadns.net/www"] = burstRows("2026-10-02T21:12:00Z", 30)
	reg := prometheus.NewRegistry()
	c := NewLivenessTrafficCollector(f.pool(t, 1), reg, livenessConfig("example.akadns.net", "www"), "akamai_gtm_", ts("2026-10-02T21:00:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))

	assert.Len(t, collectSamples(t, c), 2, "failures + duration for the one series")
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		m := mf.GetMetric()[0]
		assert.Equal(t, uint64(30), m.GetHistogram().GetSampleCount()+m.GetSummary().GetSampleCount(), mf.GetName())
	}
	assert.Equal(t, 1, f.callCount("live-report"))
}

// Rows older than the exposure behind the window end are counted, never shown as current.
func TestLivenessCollectorRestartDoesNotReplayOldErrors(t *testing.T) {
	f := newFakeAPI(t)
	f.livenessWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T21:58:00Z")}
	f.liveRows["example.akadns.net/www"] = burstRows("2026-10-02T21:12:00Z", 30)
	reg := prometheus.NewRegistry()
	c := NewLivenessTrafficCollector(f.pool(t, 1), reg, livenessConfig("example.akadns.net", "www"), "akamai_gtm_", ts("2026-10-02T21:00:00Z"), time.Hour, testOpts)
	require.NoError(t, c.poll(context.Background()))

	assert.Empty(t, collectSamples(t, c))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 2)
}

func TestLivenessExposureFollowsReportTime(t *testing.T) {
	c := NewLivenessTrafficCollector(nil, prometheus.NewRegistry(), livenessConfig("example.akadns.net", "www"), "akamai_gtm_", ts("2026-10-02T21:00:00Z"), time.Hour, testOpts)
	now := time.Now()
	c.processRow(c.targets[0], liveRow("2026-10-02T21:12:00Z", 3135, 2503, 60), ts("2026-10-02T21:12:00Z"), ts("2026-10-02T21:14:00Z"), now)
	require.Len(t, c.cache.entries, 2)
	for _, e := range c.cache.entries {
		assert.Equal(t, now.Add(3*time.Minute), e.expires, "exposed until the window end is 5 min past the row")
	}
}

func TestSampleCacheExpiry(t *testing.T) {
	c := newSampleCache(time.Minute)
	desc := prometheus.NewDesc("test_metric", "help", nil, nil)
	now := time.Now()
	c.put("a", prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1), now)

	count := func(at time.Time) int {
		ch := make(chan prometheus.Metric, 10)
		c.collect(ch, at)
		close(ch)
		return len(ch)
	}
	assert.Equal(t, 1, count(now.Add(59*time.Second)))
	assert.Equal(t, 0, count(now.Add(time.Minute)))
	assert.Empty(t, c.entries, "expired entries are dropped")
}

func TestAPIErrorsAreCountedAndRetriedNextCycle(t *testing.T) {
	f := newFakeAPI(t)
	f.trafficWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:30:00Z")}
	f.dcRows["example.akadns.net/3131"] = []*DatacenterTrafficData{dcRow("2026-10-02T19:25:00Z", 10)}
	// 404 for an unknown datacenter must not stop the others.
	c := NewDatacenterTrafficCollector(f.pool(t, 2), prometheus.NewRegistry(), dcConfig("example.akadns.net", 3131),
		"akamai_gtm_", ts("2026-10-02T19:20:00Z"), time.Hour, testOpts)
	c.targets[0].domain = "missing.akadns.net"
	require.NoError(t, c.poll(context.Background()))
	assert.Empty(t, collectSamples(t, c))
	assert.True(t, c.targets[0].fetchedThrough.IsZero(), "a failed fetch is retried on the next cycle")

	c.targets[0].domain = "example.akadns.net"
	require.NoError(t, c.poll(context.Background()))
	assert.Len(t, collectSamples(t, c), 1)
}

// Run with -race: scrapes run while a poll cycle writes to the cache.
func TestConcurrentScrapesDuringPoll(t *testing.T) {
	f := newFakeAPI(t)
	f.latency = 5 * time.Millisecond
	f.trafficWindow = [2]time.Time{ts("2026-07-01T00:00:00Z"), ts("2026-10-02T19:30:00Z")}
	var ids []int
	for id := 1; id <= 40; id++ {
		ids = append(ids, id)
		f.dcRows[fmt.Sprintf("example.akadns.net/%d", id)] = []*DatacenterTrafficData{
			dcRow("2026-10-02T19:20:00Z", 1), dcRow("2026-10-02T19:25:00Z", 2), dcRow("2026-10-02T19:30:00Z", 3),
		}
	}
	reg := prometheus.NewRegistry()
	c := NewDatacenterTrafficCollector(f.pool(t, 8), reg, dcConfig("example.akadns.net", ids...),
		"akamai_gtm_", ts("2026-10-02T19:15:00Z"), time.Hour, testOpts)
	reg.MustRegister(c)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_, err := reg.Gather()
				assert.NoError(t, err)
			}
		}()
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, c.poll(context.Background()))
	}
	cancel()
	wg.Wait()
	assert.LessOrEqual(t, f.maxInflight, 8, "concurrency is bounded by the pool size")
	for _, s := range collectSamples(t, c) {
		assert.Equal(t, 3.0, s.value)
	}
}
