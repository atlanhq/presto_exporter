/**
 * Copyright (C) 2018 Yahoo Japan Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"database/sql"
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"net/http"
)
import "github.com/montanaflynn/stats"
import _ "github.com/prestodb/presto-go-client/presto"

const (
	namespace = "presto_cluster"
)

type Exporter struct {
	uri              string
	connectionString string
	RunningQueries   float64 `json:"runningQueries"`
	BlockedQueries   float64 `json:"blockedQueries"`
	QueuedQueries    float64 `json:"queuedQueries"`
	ActiveWorkers    float64 `json:"activeWorkers"`
	RunningDrivers   float64 `json:"runningDrivers"`
	ReservedMemory   float64 `json:"reservedMemory"`
	TotalInputRows   float64 `json:"totalInputRows"`
	TotalInputBytes  float64 `json:"totalInputBytes"`
	TotalCpuTimeSecs float64 `json:"totalCpuTimeSecs"`
}

type WorkerStatus struct {
	MemoryInfo struct{
		Pools struct{
			General struct{
				FreeBytes int64 `json:"freeBytes"`
				MaxBytes  int64 `json:"maxBytes"`
			} `json:"general"`
		} `json:"pools"`
	} `json:"memoryInfo"`
}

var (
	runningQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "running_queries"),
		"Running requests of the presto cluster.",
		nil, nil,
	)
	blockedQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "blocked_queries"),
		"Blocked queries of the presto cluster.",
		nil, nil,
	)
	queuedQueries = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "queued_queries"),
		"Queued queries of the presto cluster.",
		nil, nil,
	)
	activeWorkers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "active_workers"),
		"Active workers of the presto cluster.",
		nil, nil,
	)
	runningDrivers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "running_drivers"),
		"Running drivers of the presto cluster.",
		nil, nil,
	)
	reservedMemory = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "reserved_memory"),
		"Reserved memory of the presto cluster.",
		nil, nil,
	)
	totalInputRows = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_input_rows"),
		"Total input rows of the presto cluster.",
		nil, nil,
	)
	totalInputBytes = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_input_bytes"),
		"Total input bytes of the presto cluster.",
		nil, nil,
	)
	totalCpuTimeSecs = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_cpu_time_secs"),
		"Total cpu time of the presto cluster.",
		nil, nil,
	)

	averageGeneralPoolMemFree = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "average_general_mem_free"),
		"Average general pool free memory in presto cluster",
		nil, nil,
	)

	averageGeneralPoolMemMax = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "average_general_mem_max"),
		"Average general pool max memory in presto cluster",
		nil, nil,
	)

	totalGeneralPoolMemFree = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_general_mem_free"),
		"total general pool free memory in presto cluster",
		nil, nil,
	)

	totalGeneralPoolMemMax = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_general_mem_max"),
		"total general pool max memory in presto cluster",
		nil, nil,
	)
	totalWorkers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "total_workers"),
		"total workers in presto cluster",
		nil, nil,
	)
	medianGeneralPoolMemFree = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "median_general_mem_free"),
		"median general pool free memory in presto cluster",
		nil, nil,
	)
)

// Describe implements the prometheus.Collector interface.
func (e Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- runningQueries
	ch <- blockedQueries
	ch <- queuedQueries
	ch <- activeWorkers
	ch <- runningDrivers
	ch <- reservedMemory
	ch <- totalInputRows
	ch <- totalInputBytes
	ch <- totalCpuTimeSecs
	ch <- averageGeneralPoolMemMax
	ch <- averageGeneralPoolMemFree
	ch <- totalGeneralPoolMemFree
	ch <- totalGeneralPoolMemMax
	ch <- totalWorkers
	ch <- medianGeneralPoolMemFree
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address on which to expose metrics and web interface.").Default(":9483").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		opts          = Exporter{}
	)
	kingpin.Flag("web.connUri", "connection string").Default("http://user@localhost:8080?catalog=default&schema=test").StringVar(&opts.connectionString)
	kingpin.Flag("web.url", "Presto cluster address.").Default("http://localhost:8080/v1/cluster").StringVar(&opts.uri)

	log.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("presto_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	log.Infoln("Starting presto_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	prometheus.MustRegister(&Exporter{uri: opts.uri, connectionString: opts.connectionString})

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Presto Exporter</title></head>
			<body>
			<h1>Presto Exporter</h1>
			<p><a href="` + *metricsPath + `">Metrics</a></p>
			</body>
			</html>`))
	})

	log.Infoln("Listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

// Collect implements the prometheus.Collector interface.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	resp, err := http.Get(e.uri + "cluster")
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	db, err := sql.Open("presto", e.connectionString)
	var data, err2 = db.Query("select * from nodes")
	if err2 != nil {
		log.Error("Can't get node information")
	}

	var results [][]string

	for data.Next() {
		dest := make([]string, 5)
		err := data.Scan(&dest[0], &dest[1], &dest[2], &dest[3], &dest[4])
		if err != nil {
			log.Error(err)
		}
		results = append(results, dest)
	}
	averageGeneralMemFree := int64(0)
	averageGeneralMemMax := int64(0)
	totalGeneralMemFree := int64(0)
	totalGeneralMemMax := int64(0)
	var totalGeneralMemFreeArray []float64
	workerCount := int64(0)
	for _, result := range results {
		if result[3] == "true" {
			continue
		}
		if result[4] == "shutting_down" {
			continue
		}
		resp2, err2 := http.Get(e.uri + "worker/" + result[0] + "/status")
		if err2 != nil {
			log.Errorf("%s", err2)
			continue
		}
		body, err := ioutil.ReadAll(resp2.Body)
		if err != nil {
			log.Errorf("%s", err)
			continue
		}

		workerStatus := WorkerStatus{}
		err3 := json.Unmarshal(body, &workerStatus)
		if err3 != nil {
			log.Errorf("%s", err)
			log.Error(body)
			continue
		}
		totalGeneralMemFreeArray = append(totalGeneralMemFreeArray, float64(totalGeneralMemFree))
		totalGeneralMemFree = totalGeneralMemFree + workerStatus.MemoryInfo.Pools.General.FreeBytes
		totalGeneralMemMax = totalGeneralMemMax + workerStatus.MemoryInfo.Pools.General.MaxBytes
		workerCount = workerCount + 1
		//log.Info("free: ", workerStatus.MemoryInfo.Pools.General.FreeBytes, "id: ", result[0])
		//log.Info("not free: ", workerStatus.MemoryInfo.Pools.General.MaxBytes, "id: ", result[0])
	}
	//log.Info("worker count ", workerCount)
	averageGeneralMemFree = totalGeneralMemFree / workerCount
	averageGeneralMemMax = totalGeneralMemMax / workerCount
	medianGeneralMemFree, _ := stats.Median(totalGeneralMemFreeArray)

	if resp.StatusCode != 200 {
		log.Errorf("%s", err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("%s", err)
		return
	}
	err = json.Unmarshal(body, &e)
	if err != nil {
		log.Errorf("%s", err)
		return
	}

	ch <- prometheus.MustNewConstMetric(runningQueries, prometheus.GaugeValue, e.RunningQueries)
	ch <- prometheus.MustNewConstMetric(blockedQueries, prometheus.GaugeValue, e.BlockedQueries)
	ch <- prometheus.MustNewConstMetric(queuedQueries, prometheus.GaugeValue, e.QueuedQueries)
	ch <- prometheus.MustNewConstMetric(activeWorkers, prometheus.GaugeValue, e.ActiveWorkers)
	ch <- prometheus.MustNewConstMetric(runningDrivers, prometheus.GaugeValue, e.RunningDrivers)
	ch <- prometheus.MustNewConstMetric(reservedMemory, prometheus.GaugeValue, e.ReservedMemory)
	ch <- prometheus.MustNewConstMetric(totalInputRows, prometheus.GaugeValue, e.TotalInputRows)
	ch <- prometheus.MustNewConstMetric(totalInputBytes, prometheus.GaugeValue, e.TotalInputBytes)
	ch <- prometheus.MustNewConstMetric(totalCpuTimeSecs, prometheus.GaugeValue, e.TotalCpuTimeSecs)
	ch <- prometheus.MustNewConstMetric(averageGeneralPoolMemFree, prometheus.GaugeValue, float64(averageGeneralMemFree))
	ch <- prometheus.MustNewConstMetric(averageGeneralPoolMemMax, prometheus.GaugeValue, float64(averageGeneralMemMax))
	ch <- prometheus.MustNewConstMetric(totalGeneralPoolMemFree, prometheus.GaugeValue, float64(totalGeneralMemFree))
	ch <- prometheus.MustNewConstMetric(totalGeneralPoolMemMax, prometheus.GaugeValue, float64(totalGeneralMemMax))
	ch <- prometheus.MustNewConstMetric(totalWorkers, prometheus.GaugeValue, float64(workerCount))
	ch <- prometheus.MustNewConstMetric(medianGeneralPoolMemFree, prometheus.GaugeValue, medianGeneralMemFree)
}
