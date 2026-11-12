package burrow_exporter

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var groupMetrics = []*prometheus.GaugeVec{
	KafkaConsumerPartitionLag,
	KafkaConsumerPartitionCurrentOffset,
	KafkaConsumerPartitionCurrentStatus,
	KafkaConsumerPartitionMaxOffset,
	KafkaConsumerTotalLag,
	KafkaConsumerStatus,
}

var topicMetrics = []*prometheus.GaugeVec{
	KafkaTopicPartitionOffset,
}

// exportedSeries remembers which groups and topics each cluster listed on the
// previous scrape, so that series for a group or topic Burrow no longer lists
// are deleted instead of being exported with their last value forever.
type exportedSeries struct {
	lock   sync.Mutex
	groups map[string]map[string]struct{}
	topics map[string]map[string]struct{}
}

func newExportedSeries() *exportedSeries {
	return &exportedSeries{
		groups: map[string]map[string]struct{}{},
		topics: map[string]map[string]struct{}{},
	}
}

func (e *exportedSeries) replace(cluster string, listedGroups, listedTopics []string) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.groups[cluster] = deleteDeparted(e.groups[cluster], listedGroups, cluster, "group", groupMetrics)
	e.topics[cluster] = deleteDeparted(e.topics[cluster], listedTopics, cluster, "topic", topicMetrics)
}

func deleteDeparted(previous map[string]struct{}, listed []string, cluster, label string, metrics []*prometheus.GaugeVec) map[string]struct{} {
	current := make(map[string]struct{}, len(listed))
	for _, name := range listed {
		current[name] = struct{}{}
	}

	for name := range previous {
		if _, stillListed := current[name]; stillListed {
			continue
		}
		for _, metric := range metrics {
			metric.DeletePartialMatch(prometheus.Labels{"cluster": cluster, label: name})
		}
	}

	return current
}
