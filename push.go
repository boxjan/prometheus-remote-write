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
	"math"
	"net/http"
	"strings"
	"time"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Pusher struct {
	error      error
	urls       []string
	gatherers  prometheus.Gatherers
	registerer prometheus.Registerer
	interval   int

	client             HTTPDoer
	useBasicAuth       bool
	username, password string

	expfmt    expfmt.Format
	labels    map[string]string
	snappyBuf *bytes.Buffer
}

func New() *Pusher {
	var (
		reg = prometheus.NewRegistry()
	)

	return &Pusher{
		gatherers:  prometheus.Gatherers{reg},
		registerer: reg,
		client:     &http.Client{},
		expfmt:     expfmt.FmtProtoDelim,
		labels:     map[string]string{},
	}
}

func (p *Pusher) BasicAuth(username, password string) *Pusher {
	p.useBasicAuth = true
	p.username = username
	p.password = password
	return p
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
func (p *Pusher) Push(url string) error {
	if err := p.Collect(); err != nil {
		return err
	}
	return p.PushLocal(url)
}

// Add works like push, but only previously pushed metrics with the same name
// (and the same job and other grouping labels) will be replaced. (It uses HTTP
// method “POST” to push to the Pushgateway.)
func (p *Pusher) Add(url string) error {
	if err := p.Collect(); err != nil {
		return err
	}
	return p.AddLocal(url)
}

// PushLocal push local snappy buffer to given url
func (p *Pusher) PushLocal(url string) error {
	return p.push(url, http.MethodPost)
}

// AddLocal add local snappy buffer to given url
func (p *Pusher) AddLocal(url string) error {
	return p.push(url, http.MethodPut)
}

// Collect will generate a prompb.WriteRequest with snappy compress and save in
// memory, use PushLocal or AddLocal to write same.
func (p *Pusher) Collect() error {
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

	p.snappyBuf = &bytes.Buffer{}
	p.snappyBuf.Write(snappy.Encode(nil, data))
	return nil
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

func (p *Pusher) push(url, method string) error {
	if p.snappyBuf == nil || p.snappyBuf.Len() == 0 {
		return fmt.Errorf("empty snappy buf")
	}

	if !strings.Contains(url, "://") {
		url = "http://" + url
	}
	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}

	req, err := http.NewRequest(method, url, p.snappyBuf)
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
		return fmt.Errorf("unexpected status code %d while pushing to %s: %s", resp.StatusCode, url, body)
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
					samples = append(samples, prompb.Sample{Value: m.GetCounter().GetValue(), Timestamp: t})
				case io_prometheus_client.MetricType_GAUGE:
					samples = append(samples, prompb.Sample{Value: m.GetGauge().GetValue(), Timestamp: t})
				case io_prometheus_client.MetricType_UNTYPED:
					samples = append(samples, prompb.Sample{Value: m.GetUntyped().GetValue(), Timestamp: t})
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

	var promTs = []prompb.TimeSeries{sum, count}
	for _, q := range m.Summary.Quantile {
		labels := p.getMetricLabels(mf, m)
		labels = append(labels, prompb.Label{Name: "quantile", Value: fmt.Sprintf("%g", q.GetQuantile())})
		samples := []prompb.Sample{{Value: q.GetValue(), Timestamp: t}}
		promTs = append(promTs, prompb.TimeSeries{Labels: labels, Samples: samples})
	}
	return promTs
}

func (p *Pusher) parseMetricTypeHistogram(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) []prompb.TimeSeries {
	sum := p.getMetricTypeHistogramSum(mf, m, t)
	count := p.getMetricTypeHistogramCount(mf, m, t)

	var promTs = []prompb.TimeSeries{sum, count}
	for _, b := range m.GetHistogram().GetBucket() {
		labels := p.getMetricLabels(mf, m)
		labels = append(labels, prompb.Label{Name: "le", Value: fmt.Sprintf("%g", b.GetUpperBound())})
		samples := []prompb.Sample{{Value: float64(b.GetCumulativeCount()), Timestamp: t}}
		promTs = append(promTs, prompb.TimeSeries{Labels: labels, Samples: samples})
	}
	labels := p.getMetricLabels(mf, m)
	labels = append(labels, prompb.Label{Name: "le", Value: fmt.Sprintf("%g", math.Inf(1))})
	samples := []prompb.Sample{{Value: float64(m.GetHistogram().GetSampleCount()), Timestamp: t}}
	promTs = append(promTs, prompb.TimeSeries{Labels: labels, Samples: samples})
	return []prompb.TimeSeries{sum, count}
}

func (p *Pusher) getMetricTypeHistogramSum(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) prompb.TimeSeries {
	return prompb.TimeSeries{Labels: p.getMetricLabelsNameWithWithSuffix(mf, m, "sum"),
		Samples: []prompb.Sample{{Value: m.GetHistogram().GetSampleSum(), Timestamp: t}}}
}

func (p *Pusher) getMetricTypeHistogramCount(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) prompb.TimeSeries {
	return prompb.TimeSeries{Labels: p.getMetricLabelsNameWithWithSuffix(mf, m, "count"),
		Samples: []prompb.Sample{{Value: float64(m.GetHistogram().GetSampleCount()), Timestamp: t}}}
}

func (p *Pusher) getMetricTypeSummarySum(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) prompb.TimeSeries {
	return prompb.TimeSeries{Labels: p.getMetricLabelsNameWithWithSuffix(mf, m, "sum"),
		Samples: []prompb.Sample{{Value: m.GetSummary().GetSampleSum(), Timestamp: t}}}
}

func (p *Pusher) getMetricTypeSummaryCount(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, t int64) prompb.TimeSeries {
	return prompb.TimeSeries{Labels: p.getMetricLabelsNameWithWithSuffix(mf, m, "count"),
		Samples: []prompb.Sample{{Value: float64(m.GetSummary().GetSampleCount()), Timestamp: t}}}
}

func (p *Pusher) getMetricLabels(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric) []prompb.Label {
	return p.getMetricLabelsNameWithWithSuffix(mf, m, "")
}

func (p *Pusher) getMetricLabelsNameWithWithSuffix(mf *io_prometheus_client.MetricFamily, m *io_prometheus_client.Metric, suffix string) []prompb.Label {
	labels := make([]prompb.Label, 0, 1)
	if suffix != "" {
		if strings.HasPrefix(suffix, "_") {
			suffix = "_" + suffix
		}
	}
	labels = append(labels, prompb.Label{Name: "__name__", Value: mf.GetName() + suffix})
	for _, l := range m.GetLabel() {
		labels = append(labels, prompb.Label{Name: *l.Name, Value: *l.Value})
	}
	for k, v := range p.labels {
		labels = append(labels, prompb.Label{Name: k, Value: v})
	}
	return labels
}
