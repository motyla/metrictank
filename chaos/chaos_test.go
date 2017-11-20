package chaos

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/grafana/metrictank/chaos/out/kafkamdm"
	"github.com/raintank/met/helper"
	"gopkg.in/raintank/schema.v1"
)

// TODO: cleanup when ctrl-C go test (teardown all containers)

const numPartitions = 12

var tracker *Tracker
var metrics []*schema.MetricData

func init() {
	for i := 0; i < numPartitions; i++ {
		name := fmt.Sprintf("some.id.of.a.metric.%d", i)
		m := &schema.MetricData{
			OrgId:    1,
			Name:     name,
			Metric:   name,
			Interval: 1,
			Value:    1,
			Unit:     "s",
			Mtype:    "gauge",
		}
		m.SetId()
		metrics = append(metrics, m)
	}
}

func TestMain(m *testing.M) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, path("docker/launch.sh"), "docker-chaos")
	cmd.Env = append(cmd.Env, "MT_CLUSTER_MIN_AVAILABLE_SHARDS=12")

	var err error
	tracker, err = NewTracker(cmd, false, false, "launch-stdout", "launch-stderr")
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	retcode := m.Run()

	fmt.Println("stopping the docker-compose stack...")
	cancelFunc()
	if err := cmd.Wait(); err != nil {
		log.Printf("ERROR: could not cleanly shutdown running docker-compose command: %s", err)
		retcode = 1
	}

	os.Exit(retcode)
}

func TestClusterStartup(t *testing.T) {
	// wait until MT's are up and connected to kafka and cassandra
	matchers := []Matcher{
		{
			Str: "metrictank0_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank1_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank2_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank3_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank4_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank5_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "grafana.*Initializing HTTP Server.*:3000",
		},
	}
	ch := tracker.Match(matchers)
	select {
	case <-ch:
		postAnnotation("TestClusterStartup:OK")
		return
	case <-time.After(time.Second * 40):
		postAnnotation("TestClusterStartup:FAIL")
		t.Fatal("timed out while waiting for all metrictank instances to come up")
	}
}

// 1 metric to each of 12 partitions, each partition replicated twice = expect total workload across cluster of 24Hz
func TestClusterBaseIngestWorkload(t *testing.T) {
	postAnnotation("TestClusterBaseIngestWorkload:begin")

	//	tracker.LogStdout(true)
	//	tracker.LogStderr(true)

	go func() {
		t.Log("Starting kafka publishing")
		stats, _ := helper.New(false, "", "standard", "", "")
		out, err := kafkamdm.New("mdm", []string{"localhost:9092"}, "none", stats, "lastNum")
		if err != nil {
			log.Fatal(4, "failed to create kafka-mdm output. %s", err)
		}
		ticker := time.NewTicker(time.Second)

		for tick := range ticker.C {
			unix := tick.Unix()
			for i := range metrics {
				metrics[i].Time = unix
			}
			err := out.Flush(metrics)
			if err != nil {
				t.Fatalf("failed to send data to kafka: %s", err)
			}
		}
	}()

	suc6, resp := retryGraphite("perSecond(metrictank.stats.docker-cluster.*.input.kafka-mdm.metrics_received.counter32)", "-5s", 15, func(resp response) bool {
		exp := []string{
			"perSecond(metrictank.stats.docker-cluster.metrictank0.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank1.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank2.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank3.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank4.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank5.input.kafka-mdm.metrics_received.counter32)",
		}
		if !validateTargets(exp)(resp) {
			return false
		}
		for _, series := range resp.r {
			var sum float64
			if len(series.Datapoints) != 5 {
				return false
			}
			// skip the first point. it always seems to be null for some reason
			for _, p := range series.Datapoints[1:] {
				if math.IsNaN(p.Val) {
					return false
				}
				sum += p.Val
			}
			// avg of all (4) datapoints must be 4 (metrics ingested per second by each instance)
			if sum/4 != 4 {
				return false
			}
		}
		return true
	})
	if !suc6 {
		postAnnotation("TestClusterBaseIngestWorkload:FAIL")
		t.Fatalf("cluster did not reach a state where each MT instance receives 4 points per second. last response was: %s", spew.Sdump(resp))
	}

	suc6, resp = retryMT("sum(some.id.of.a.metric.*)", "-10s", 14, validateCorrect(12))
	if !suc6 {
		postAnnotation("TestClusterBaseIngestWorkload:FAIL")
		t.Fatalf("could not query correct result set. sum of 12 series, each valued 1, should result in 12.  last response was: %s", spew.Sdump(resp))
	}
	postAnnotation("TestClusterBaseIngestWorkload:OK")
}

func TestQueryWorkload(t *testing.T) {
	postAnnotation("TestQueryWorkload:begin")
	pre := time.Now()
	rand.Seed(pre.Unix())

	results := checkMT([]int{6060, 6061, 6062, 6063, 6064, 6065}, "sum(some.id.of.a.metric.*)", "-10s", time.Minute, 6000, validateCorrect(12))

	exp := checkResults{
		valid:   []int{6000},
		empty:   0,
		timeout: 0,
		other:   0,
	}
	if !reflect.DeepEqual(exp, results) {
		postAnnotation("TestQueryWorkload:FAIL")
		t.Fatalf("expected only correct results. got %s", spew.Sdump(results))
	}
	postAnnotation("TestQueryWorkload:OK")
}

// TestIsolateOneInstance tests what happens during the isolation of one instance, when min-available-shards is 12
// this should happen:
// at all times, all queries to all of the remaining nodes should be successful
// since they have at least 1 instance running for each shard.
// the isolated shard should either return correct replies, or errors (in two cases: when it marks any shards as down,
// but also before it does, but fails to get data via clustered requests from peers)
//. TODO: in production do we stop querying isolated peers?
func TestIsolateOneInstance(t *testing.T) {
	postAnnotation("TestIsolateOneInstance:begin")
	t.Log("Starting TestIsolateOneInstance)")
	//	tracker.LogStdout(true)
	//	tracker.LogStderr(true)
	pre := time.Now()
	rand.Seed(pre.Unix())
	numReqMt4 := 1200

	mt4ResultsChan := make(chan checkResults, 1)
	otherResultsChan := make(chan checkResults, 1)

	go func() {
		mt4ResultsChan <- checkMT([]int{6064}, "sum(some.id.of.a.metric.*)", "-10s", time.Minute, numReqMt4, validateCorrect(12), validateCode(503))
	}()
	go func() {
		otherResultsChan <- checkMT([]int{6060, 6061, 6062, 6063, 6065}, "sum(some.id.of.a.metric.*)", "-10s", time.Minute, 6000, validateCorrect(12))
	}()

	// now go ahead and isolate for 30s
	isolate("metrictank4", "30s", "metrictank0", "metrictank1", "metrictank2", "metrictank3", "metrictank5")

	// collect results of the minute long experiment
	mt4Results := <-mt4ResultsChan
	otherResults := <-otherResultsChan

	// validate results of isolated node
	if mt4Results.valid[0]+mt4Results.valid[1] != numReqMt4 {
		t.Fatalf("expected mt4 to return either correct or erroring responses. got %s", spew.Sdump(mt4Results))
	}
	if mt4Results.valid[1] < numReqMt4*30/100 {
		// the instance is completely down for 30s of the 60s experiment run, but we allow some slack
		t.Fatalf("expected at least 30%% of all mt4 results to succeed. got %s", spew.Sdump(mt4Results))
	}

	// validate results of other cluster nodes
	exp := checkResults{
		valid:   []int{6000},
		empty:   0,
		timeout: 0,
		other:   0,
	}
	if !reflect.DeepEqual(exp, otherResults) {
		postAnnotation("TestIsolateOneInstance:FAIL")
		t.Fatalf("expected only correct results for all cluster nodes. got %s", spew.Sdump(otherResults))
	}
	postAnnotation("TestIsolateOneInstance:OK")
}

func TestHang(t *testing.T) {
	postAnnotation("TestHang:begin")
	t.Log("whatever happens, keep hanging for now, so that we can query grafana dashboards still")
	var ch chan struct{}
	<-ch
}

// maybe useful in the future, test also clean exit and rejoin like so:
//stop("metrictank4")
//time.AfterFunc(30*time.Second, func() {
//	start("metrictank4")
//})
