package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/getsentry/raven-go"
	"github.com/golang/glog"
	_ "log"
	"net"
	"net/http"
	_ "regexp"
	"strings"
	"sync"
	"time"
)

type (
	Consumers struct {
		Consumers []Consumer `json:"consumers"`
	}

	Consumer struct {
		Name   string   `json:"name"`
		Type   string   `json:"type"`
		Topics []string `json:"topics"`
	}

	Clusters struct {
		Clusters ClusterInfo
	}

	ClusterInfo struct {
		Active []Cluster
	}

	Cluster struct {
		Name                     string
		CuratorConfig            CuratorConfig
		Enabled                  bool
		JmxEnabled               bool
		PoolConsumers            bool
		ActiveOffsetCacheEnabled bool
	}

	CuratorConfig struct {
		ZkConnect       string
		ZkMaxRetry      int
		BaseSleepTimeMs int
		MaxSleepTimeMs  int
	}

	ConsumerInfo struct {
		TotalLag               int64    `json:"totalLag"`
		PercentageCovered      int      `json:"percentageCovered"`
		PartitionOffsets       []int64  `json:"partitionOffsets"`
		PartitionLatestOffsets []int64  `json:"partitionLatestOffsets"`
		Owners                 []string `json:"owners"`
	}
)

var (
	myClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   20,
			MaxIdleConns:          100,
			ResponseHeaderTimeout: 20 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
		},
		Timeout: 30 * time.Second,
	}

	clustersURL = "%s/api/status/clusters"

	consumersURL = "%s/api/status/%s/consumersSummary"

	consumerSummaryURL = "%s/api/status/%s/%s/%s/groupSummary"
)

func getJson(url string, target interface{}) error {
	r, err := myClient.Get(url)
	if err != nil {
		raven.CaptureErrorAndWait(err, nil)
		return err
	}
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(target)
}

func main() {
	httpPort := flag.String("http.port", ":8080", "HttpPort")
	kafkaManagerUrl := flag.String("kafka-manager", "", "Kafka Manager URL")
	flag.Parse()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprintf(w, "ok")
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprintf(w, "ok")
	})
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		clustersInfo := new(Clusters)
		var err = getJson(fmt.Sprintf(clustersURL, *kafkaManagerUrl), clustersInfo)
		if err != nil {
			glog.Error("Error !!!%s \n ", err)
			raven.CaptureErrorAndWait(err, nil)
			return
		}
		glog.Info("Startup !!!\n")
		// TODO remove Mutex
		// TODO Change to chanel
		m := new(sync.Mutex)
		wait := new(sync.WaitGroup)
		wait.Add(len(clustersInfo.Clusters.Active))

		for idx, cluster := range clustersInfo.Clusters.Active {
			go func(cluster Cluster) {
				glog.Info("%d %s %+v\n", idx, cluster.Name, cluster)
				defer wait.Done()

				consumers := new(Consumers)
				err = getJson(fmt.Sprintf(consumersURL, *kafkaManagerUrl, cluster.Name), consumers)
				if err != nil {
					glog.Infof("Error !!!%s \n ", err)
					raven.CaptureErrorAndWait(err, nil)
					return
				}
				wait.Add(len(consumers.Consumers))

				for _, consumer := range consumers.Consumers {
					go func(w http.ResponseWriter, consumer Consumer) {
						var buffer strings.Builder

						defer wait.Done()
						if !strings.HasPrefix(consumer.Name, "console-") {
							consumerSummary := new(map[string]ConsumerInfo)
							err = getJson(fmt.Sprintf(consumerSummaryURL, *kafkaManagerUrl, cluster.Name, consumer.Name, consumer.Type), consumerSummary)
							if err != nil {
								glog.Errorf("Error !!!%s \n ", err)
								raven.CaptureErrorAndWait(err, nil)
							} else {
								fmt.Fprintf(&buffer, "# HELP kafka_manager_total_lags %s %s Number of Consuemr Lag\n", cluster.Name, consumer.Name)
								fmt.Fprintf(&buffer, "# TYPE kafka_manager_total_lags guage\n")
								for consumerName, consumerInfo := range *consumerSummary {
									fmt.Fprintf(&buffer, strings.Replace(`kafka_manager_total_lags{cluster="%s",topic="%s",consumer="%s"} %d\n`, `\n`, "\n", -1), cluster.Name, consumerName, consumer.Name, consumerInfo.TotalLag)
								}
							}
						}
						// TODO remove Mutex
						m.Lock()
						w.Write([]byte(buffer.String()))
						m.Unlock()
					}(w, consumer)
				}
			}(cluster)
		}
		wait.Wait()
	})

	glog.Info("starting kafka_manager_exporter on ", *httpPort)

	glog.Fatal(http.ListenAndServe(*httpPort, nil))

	glog.Info("Shutdown !!!\n")

}
