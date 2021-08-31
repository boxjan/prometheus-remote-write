// Package push
// Copyright 2021 boxjan
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package push

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/prompb"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Pusher struct {
	error      error
	url, job   string
	gatherers  prometheus.Gatherers
	registerer prometheus.Registerer

	client             HTTPDoer
	useBasicAuth       bool
	username, password string

	expfmt expfmt.Format
	labels map[string]string
}

func New(url string) *Pusher {
	var (
		reg = prometheus.NewRegistry()
	)

	if !strings.Contains(url, "://") {
		url = "http://" + url
	}
	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}

	return &Pusher{
		url:        url,
		gatherers:  prometheus.Gatherers{reg},
		registerer: reg,
		client:     &http.Client{},
		expfmt:     expfmt.FmtProtoDelim,
		labels:     map[string]string{},
	}
}

// Push collects/gathers all metrics from all Collectors and Gatherers added to
// this Pusher. Then, it pushes them to the Pushgateway configured while
// creating this Pusher, using the configured job name and any added grouping
// labels as grouping key. All previously pushed metrics with the same job and
// other grouping labels will be replaced with the metrics pushed by this
// call. (It uses HTTP method “PUT” to push to the Pushgateway.)
//
// Push returns the first error encountered by any method call (including this
// one) in the lifetime of the Pusher.
func (p *Pusher) Push() error {
	return p.push(http.MethodPut)
}

// Add works like push, but only previously pushed metrics with the same name
// (and the same job and other grouping labels) will be replaced. (It uses HTTP
// method “POST” to push to the Pushgateway.)
func (p *Pusher) Add() error {
	return p.push(http.MethodPost)
}

// Gatherer adds a Gatherer to the Pusher, from which metrics will be gathered
// to push them to the Pushgateway. The gathered metrics must not contain a job
// label of their own.
//
// For convenience, this method returns a pointer to the Pusher itself.
func (p *Pusher) Gatherer(g prometheus.Gatherer) *Pusher {
	p.gatherers = append(p.gatherers, g)
	return p
}

// ExtraLabel adds more label to Pusher, for add extra label when push to storage.
//
// For convenience, this method returns a pointer to the Pusher itself.
func (p *Pusher) ExtraLabel(key, values string) *Pusher {
	p.labels[key] = values
	return p
}

// Collector adds a Collector to the Pusher, from which metrics will be
// collected to push them to the Pushgateway. The collected metrics must not
// contain a job label of their own.
//
// For convenience, this method returns a pointer to the Pusher itself.
func (p *Pusher) Collector(c prometheus.Collector) *Pusher {
	if p.error == nil {
		p.error = p.registerer.Register(c)
	}
	return p
}

func (p *Pusher) push(method string) error {
	if p.error != nil {
		return p.error
	}
	wr, err := p.toPromWriteRequest()
	if err != nil {
		return err
	}
	data, err := proto.Marshal(wr)
	if err != nil {
		return fmt.Errorf("unable to marshal protobuf: %v", err)
	}

	snappyBuf := &bytes.Buffer{}
	snappyBuf.Write(snappy.Encode(nil, data))

	req, err := http.NewRequest(method, p.url, snappyBuf)
	if err != nil {
		return err
	}

	if p.useBasicAuth {
		req.SetBasicAuth(p.username, p.password)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Depending on version and configuration of the PGW, StatusOK or StatusAccepted may be returned.
	if resp.StatusCode < 200 && resp.StatusCode > 299 {
		body, _ := ioutil.ReadAll(resp.Body) // Ignore any further error as this is for an error message only.
		return fmt.Errorf("unexpected status code %d while pushing to %s: %s", resp.StatusCode, p.url, body)
	}
	return nil
}

func (p *Pusher) Format(format expfmt.Format) *Pusher {
	p.expfmt = format
	return p
}

func (p *Pusher) toPromWriteRequest() (*prompb.WriteRequest, error) {
	mfs, err := p.gatherers.Gather()
	if err != nil {
		return nil, err
	}

	promTs := make([]prompb.TimeSeries, 0, 16)
	t := time.Now().UnixNano() / int64(time.Millisecond)
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			samples := make([]prompb.Sample, 0, 1)
			var res []prompb.TimeSeries
			if *mf.Type == io_prometheus_client.MetricType_SUMMARY {
				res = p.parseMetricTypeSummary(mf, m, t)
			} else if *mf.Type == io_prometheus_client.MetricType_HISTOGRAM {
				res = p.parseMetricTypeHistogram(mf, m, t)
			} else {
				labels := p.getMetricLabels(mf, m)
				switch *mf.Type {
				case io_prometheus_client.MetricType_COUNTER:
					samples = append(samples, prompb.Sample{Value: *m.Counter.Value, Timestamp: t})
				case io_prometheus_client.MetricType_GAUGE:
					samples = append(samples, prompb.Sample{Value: *m.Gauge.Value, Timestamp: t})
				case io_prometheus_client.MetricType_UNTYPED:
					samples = append(samples, prompb.Sample{Value: *m.Untyped.Value, Timestamp: t})
				}
				res = []prompb.TimeSeries{{Labels: labels, Samples: samples}}
			}
			promTs = append(promTs, res...)
		}

	}

	return &prompb.WriteRequest{
		Timeseries: promTs,
	}, nil
}

func (p *Pusher) parseMetricTypeSummary(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) []prompb.TimeSeries {
	sum := p.getMetricTypeSummarySum(mf, m, t)
	count := p.getMetricTypeSummaryCount(mf, m, t)
	return []prompb.TimeSeries{sum, count}
}

func (p *Pusher) parseMetricTypeHistogram(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) []prompb.TimeSeries {
	sum := p.getMetricTypeHistogramSum(mf, m, t)
	count := p.getMetricTypeHistogramCount(mf, m, t)
	return []prompb.TimeSeries{sum, count}
}

func (p *Pusher) getMetricTypeSummarySum(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) prompb.TimeSeries {
	labels := p.getMetricLabels(mf, m)
	labels = append(labels, prompb.Label{Name: "type", Value: "summary_sample_sum"})
	return prompb.TimeSeries{Labels: labels, Samples: []prompb.Sample{{Value: *m.GetSummary().SampleSum, Timestamp: t}}}
}

func (p *Pusher) getMetricTypeSummaryCount(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) prompb.TimeSeries {
	labels := p.getMetricLabels(mf, m)
	labels = append(labels, prompb.Label{Name: "type", Value: "summary_sample_count"})
	return prompb.TimeSeries{Labels: labels, Samples: []prompb.Sample{{Value: float64(*m.GetSummary().SampleCount), Timestamp: t}}}
}

func (p *Pusher) getMetricTypeHistogramSum(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) prompb.TimeSeries {
	labels := p.getMetricLabels(mf, m)
	labels = append(labels, prompb.Label{Name: "type", Value: "summary_sample_sum"})
	return prompb.TimeSeries{Labels: labels, Samples: []prompb.Sample{{Value: *m.GetHistogram().SampleSum, Timestamp: t}}}
}

func (p *Pusher) getMetricTypeHistogramCount(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) prompb.TimeSeries {
	labels := p.getMetricLabels(mf, m)
	labels = append(labels, prompb.Label{Name: "type", Value: "summary_sample_count"})
	return prompb.TimeSeries{Labels: labels, Samples: []prompb.Sample{{Value: float64(*m.GetHistogram().SampleCount), Timestamp: t}}}
}

func (p *Pusher) getMetricLabels(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric) []prompb.Label {
	labels := make([]prompb.Label, 0, 1)
	labels = append(labels, prompb.Label{Name: "__name__", Value: mf.GetName()})
	for _, l := range m.GetLabel() {
		labels = append(labels, prompb.Label{Name: *l.Name, Value: *l.Value})
	}
	for k, v := range p.labels {
		labels = append(labels, prompb.Label{Name: k, Value: v})
	}
	return labels
}
