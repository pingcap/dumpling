// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package export

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	finishedSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "finished_size",
			Help:      "counter for dumpling finished file size",
		}, []string{})
	finishedRowsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "finished_rows",
			Help:      "counter for dumpling finished rows",
		}, []string{})
	writeTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dumpling",
			Subsystem: "write",
			Name:      "write_duration_time",
			Help:      "Bucketed histogram of write time (s) of files",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		}, []string{})
	receiveWriteChunkTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dumpling",
			Subsystem: "write",
			Name:      "receive_chunk_duration_time",
			Help:      "Bucketed histogram of write time (s) of files",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		}, []string{})
	errorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "error_count",
			Help:      "Total error count during dumping progress",
		}, []string{})
)

// RegisterMetrics registers metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(finishedSizeCounter)
	registry.MustRegister(finishedRowsCounter)
	registry.MustRegister(writeTimeHistogram)
	registry.MustRegister(receiveWriteChunkTimeHistogram)
	registry.MustRegister(errorCount)
}

func RemoveLabelValuesWithTaskInMetrics(labels prometheus.Labels) {
	finishedSizeCounter.Delete(labels)
	finishedRowsCounter.Delete(labels)
	writeTimeHistogram.Delete(labels)
	receiveWriteChunkTimeHistogram.Delete(labels)
	errorCount.Delete(labels)
}
