package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/getsentry/raven-go"
	"github.com/golang/glog"
	"log"
	_ "log"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
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
	cookieJar, _ = cookiejar.New(nil)

	myClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   20,
			MaxIdleConns:          200,
			ResponseHeaderTimeout: 20 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
		},
		Timeout: 30 * time.Second,
		Jar:     cookieJar,
	}

	authenticationName = "play-basic-authentication-filter"

	authentication = make(map[string]http.Cookie)

	clustersURL = "%s/api/status/clusters"

	consumersURL = "%s/api/status/%s/consumersSummary"

	consumerSummaryURL = "%s/api/status/%s/%s/%s/groupSummary"
)

func getJson(u string, user string, pass string, target interface{}) (interface{}, error) {
	uri, err := url.Parse(u)
	if err != nil {
		log.Fatal(err)
	}
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	auth, exists := authentication[authenticationName]
	if exists {
		fmt.Printf("setCookie  %s: %s\n", auth.Name, auth.Value)
		req.AddCookie(&auth)
	} else {
		req.SetBasicAuth(user, pass)
	}
	res, err := myClient.Do(req)
	if err != nil {
		raven.CaptureErrorAndWait(err, nil)
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == 200 {
		glog.Infof("%d: %s", res.StatusCode, u)
		for _, cookie := range cookieJar.Cookies(uri) {
			if cookie.Name == authenticationName {
				if val, ok := authentication[authenticationName]; ok {
					if val.Value != cookie.Value {
						fmt.Printf("getCookie  %s: %s\n", cookie.Name, cookie.Value)
						authentication[authenticationName] = http.Cookie{Name: authenticationName, Value: cookie.Value}
					}
				}
			}
		}
		return json.NewDecoder(res.Body).Decode(target), nil
	} else {
		glog.Errorf("Response: %d, %s ", res.StatusCode, res.Body)
		return fmt.Errorf("Response: %v", &res), nil
	}
}

func main() {
	httpPort := flag.String("http.port", ":8080", "HttpPort")
	user := flag.String("user", "", "User")
	pass := flag.String("pass", "", "Password")
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
		glog.Info("Startup !!!\n")
		start := time.Now()
		clustersInfo := new(Clusters)

		var _, err = getJson(fmt.Sprintf(clustersURL, *kafkaManagerUrl), *user, *pass, clustersInfo)
		if err != nil {
			glog.Error("Error !!!%s \n ", err)
			raven.CaptureErrorAndWait(err, nil)
			return
		}
		// TODO remove Mutex
		// TODO Change to chanel
		m := new(sync.Mutex)
		wait := new(sync.WaitGroup)

		for _, cluster := range clustersInfo.Clusters.Active {
			if cluster.Enabled {
				wait.Add(1)
				go func(cluster Cluster) {
					defer wait.Done()

					consumers := new(Consumers) // or &Foo{}
					kafkaManagerClusterUrl := fmt.Sprintf(consumersURL, *kafkaManagerUrl, cluster.Name)
					glog.Infof("Start cluster: %s", kafkaManagerClusterUrl)
					_, err = getJson(kafkaManagerClusterUrl, *user, *pass, consumers)
					if err != nil {
						glog.Infof("Error %s !!!%s \n ", kafkaManagerClusterUrl, err)
						// raven.CaptureErrorAndWait(err, nil)
						// return
					} else {
						for _, consumer := range consumers.Consumers {
							wait.Add(1)
							go func(w http.ResponseWriter, consumer Consumer) {
								var buffer strings.Builder

								defer wait.Done()
								if !strings.HasPrefix(consumer.Name, "console-") {
									consumerSummary := new(map[string]ConsumerInfo)
									consumerUrl := fmt.Sprintf(consumerSummaryURL, *kafkaManagerUrl, cluster.Name, consumer.Name, consumer.Type)
									_, err = getJson(consumerUrl, *user, *pass, consumerSummary)
									if err != nil {
										glog.Errorf("Error !!! %s: %s \n ", consumerUrl, err)
										// raven.CaptureErrorAndWait(err, nil)
										// return
									} else {
										var bufferConsumer strings.Builder
										for consumerName, consumerInfo := range *consumerSummary {
											fmt.Fprintf(&bufferConsumer, strings.Replace(`kafka_manager_total_lags{cluster="%s",topic="%s",consumer="%s"} %d\n`, `\n`, "\n", -1), cluster.Name, consumerName, consumer.Name, consumerInfo.TotalLag)
										}
										if bufferConsumer.Len() > 0 {
											fmt.Fprintf(&buffer, "# HELP kafka_manager_total_lags %s %s Number of Consuemr Lag\n", cluster.Name, consumer.Name)
											fmt.Fprintf(&buffer, "# TYPE kafka_manager_total_lags gauge\n")
											fmt.Fprint(&buffer, bufferConsumer.String())
										}
									}
								}
								// TODO remove Mutex
								if buffer.Len() > 0 {
									m.Lock()
									w.Write([]byte(buffer.String()))
									m.Unlock()
								}
							}(w, consumer)
						}
					}
				}(cluster)
			}
		}
		wait.Wait()
		elapsed := time.Since(start)
		glog.Infof("Finish !!! %s", elapsed)
	})

	glog.Info("starting kafka_manager_exporter on ", *httpPort)

	glog.Fatal(http.ListenAndServe(*httpPort, nil))

	glog.Info("Shutdown !!!\n")

}
