package burrow_exporter

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func setGroupLag(cluster, group string) {
	KafkaConsumerTotalLag.With(prometheus.Labels{"cluster": cluster, "group": group}).Set(1)
	KafkaConsumerPartitionLag.With(prometheus.Labels{"cluster": cluster, "group": group, "topic": "t", "partition": "0"}).Set(1)
}

func setTopicOffset(cluster, topic string) {
	KafkaTopicPartitionOffset.With(prometheus.Labels{"cluster": cluster, "topic": topic, "partition": "0"}).Set(1)
}

func resetAll() {
	for _, metric := range append(append([]*prometheus.GaugeVec{}, groupMetrics...), topicMetrics...) {
		metric.Reset()
	}
}

func TestReplaceDeletesSeriesOfGroupsNoLongerListed(t *testing.T) {
	resetAll()
	exported := newExportedSeries()

	setGroupLag("b", "kept")
	setGroupLag("b", "departed")
	exported.replace("b", []string{"kept", "departed"}, nil)

	setGroupLag("b", "kept")
	exported.replace("b", []string{"kept"}, nil)

	if got := testutil.CollectAndCount(KafkaConsumerTotalLag); got != 1 {
		t.Fatalf("total lag series = %d, want 1", got)
	}
	if got := testutil.CollectAndCount(KafkaConsumerPartitionLag); got != 1 {
		t.Fatalf("partition lag series = %d, want 1", got)
	}
	if got := testutil.ToFloat64(KafkaConsumerTotalLag.With(prometheus.Labels{"cluster": "b", "group": "kept"})); got != 1 {
		t.Fatalf("kept group lag = %v, want 1", got)
	}
}

func TestReplaceLeavesOtherClustersAlone(t *testing.T) {
	resetAll()
	exported := newExportedSeries()

	setGroupLag("b", "shared-name")
	setGroupLag("e", "shared-name")
	exported.replace("b", []string{"shared-name"}, nil)
	exported.replace("e", []string{"shared-name"}, nil)

	exported.replace("b", []string{}, nil)

	if got := testutil.CollectAndCount(KafkaConsumerTotalLag); got != 1 {
		t.Fatalf("total lag series = %d, want 1 (cluster e only)", got)
	}
}

func TestReplaceDeletesSeriesOfTopicsNoLongerListed(t *testing.T) {
	resetAll()
	exported := newExportedSeries()

	setTopicOffset("b", "kept")
	setTopicOffset("b", "deleted")
	exported.replace("b", nil, []string{"kept", "deleted"})
	exported.replace("b", nil, []string{"kept"})

	if got := testutil.CollectAndCount(KafkaTopicPartitionOffset); got != 1 {
		t.Fatalf("topic offset series = %d, want 1", got)
	}
}
