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

package exporter_pusher

import (
	push "github.com/boxjan/prometheus-remote-write"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"strings"
	"time"
)

var (
	reportModP         *bool
	reportEndpointsP   *string
	remoteWriteMethodP *string
	intervalP          *int
	instanceP          *string
	basicUser          *string
	basicPasswd        *string
	labelsP            *map[string]string
)

func init() {
	reportModP = kingpin.Flag("push", "will auto push data to prometheus").Default("false").Bool()
	reportEndpointsP = kingpin.Flag("push-endpoints", "push data endpoint, split by ;").
		Default("").String()
	remoteWriteMethodP = kingpin.Flag("push-method", "push data to tsdb prometheus function").
		Default("push").String()
	intervalP = kingpin.Flag("push-interval", "push data interval").Default("10").Int()
	basicUser = kingpin.Flag("basic-username", "set http basic username").Default("").String()
	basicPasswd = kingpin.Flag("basic-password", "set http basic username").Default("").String()
	instanceP = kingpin.Flag("instance", "report instance").Default("").String()
	labelsP = kingpin.Flag("label", "extra labels to differentiate instance").StringMap()
}

func ReportMod(g prometheus.Gatherer, logger log.Logger) {
	if !(*reportModP) {
		return
	}

	pusher := push.New()

	setUpBasic(pusher)
	setUpInstance(pusher, logger)
	setUpLabel(pusher)

	pusher = pusher.Gatherer(g)
	endpoints := strings.Split(*reportEndpointsP, ";")

	method := *remoteWriteMethodP
	if method != "add" && method != "push" {
		level.Error(logger).Log("msg", "unknown push method, will use push")
		method = "push"
	}
	writeEveryInterval(pusher, endpoints, method, logger)
}

func write(pusher *push.Pusher, endpoints []string, method string, logger log.Logger) {
	if err := pusher.Collect(); err != nil {
		level.Error(logger).Log("msg", "collect gatherer failed", "err", err)
	}

	for _, endpoint := range endpoints {
		var err error
		switch method {
		case "add":
			err = pusher.Add(endpoint)
		default:
			err = pusher.Push(endpoint)
		}

		if err != nil {
			level.Warn(logger).Log("msg", "push failed")
		} else {
			return
		}
	}

}

func writeEveryInterval(pusher *push.Pusher, endpoints []string, method string, logger log.Logger) {
	// first time write
	go write(pusher, endpoints, method, logger)

	for range time.NewTicker(time.Second * time.Duration(*intervalP)).C {
		go write(pusher, endpoints, method, logger)
	}
}

func setUpLabel(pusher *push.Pusher) {
	if labelsP == nil {
		return
	}

	for k, v := range *labelsP {
		pusher.ExtraLabel(k, v)
	}
}

func setUpBasic(pusher *push.Pusher) {
	if len(*basicUser) > 0 || len(*basicPasswd) > 0 {
		pusher.BasicAuth(*basicUser, *basicPasswd)
	}
}

func setUpInstance(pusher *push.Pusher, logger log.Logger) {
	var (
		instance string
	)

	if len(*instanceP) > 0 {
		instance = *instanceP
	} else {
		if hostname, err := os.Hostname(); err != nil {
			level.Error(logger).Log("msg", "get hostname failed", "err", err)
			// generate a v1 uuid to report.
			u, _ := uuid.NewUUID()
			instance = "unknown-instance:" + u.String()
		} else {
			instance = hostname
		}
	}
	level.Info(logger).Log("instance", instance)
	pusher.ExtraLabel("instance", instance)
}
