// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package geerpc

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
)

// debugText 是一个极简调试页模板，用来展示服务级和方法级的运行时指标。
const debugText = `<html>
	<body>
	<title>GeeRPC Services</title>
	<h1>GeeRPC Debug</h1>
	<p>JSON Stats: <a href="{{.JSONPath}}">{{.JSONPath}}</a></p>
	<p>
		Server Calls: {{.Calls}} |
		Errors: {{.Errors}} |
		Panics: {{.Panics}} |
		Active: {{.Active}} |
		Last Trace ID: {{.LastTraceID}} |
		Last Request ID: {{.LastRequestID}} |
		Max Concurrent: {{.MaxConcurrentRequests}} |
		Overload Rejections: {{.OverloadRejections}} |
		Avg Latency: {{.AvgLatency}} |
		Total Latency: {{.TotalLatency}} |
		Latency Buckets: {{.LatencySummary}}
	</p>
	{{range .Services}}
	<h2>Service {{.Name}}</h2>
	<p>
		Calls: {{.Calls}} |
		Errors: {{.Errors}} |
		Panics: {{.Panics}} |
		Active: {{.Active}} |
		Avg Latency: {{.AvgLatency}} |
		Total Latency: {{.TotalLatency}} |
		Latency Buckets: {{.LatencySummary}}
	</p>
	<table>
	<tr>
		<th align=center>Method</th>
		<th align=center>Calls</th>
		<th align=center>Errors</th>
		<th align=center>Panics</th>
		<th align=center>Active</th>
		<th align=center>Avg Latency</th>
		<th align=center>Total Latency</th>
		<th align=center>Latency Buckets</th>
	</tr>
	{{range .Methods}}
		<tr>
		<td align=left font=fixed>{{.Name}}({{.ArgType}}, {{.ReplyType}}) error</td>
		<td align=center>{{.Calls}}</td>
		<td align=center>{{.Errors}}</td>
		<td align=center>{{.Panics}}</td>
		<td align=center>{{.Active}}</td>
		<td align=center>{{.AvgLatency}}</td>
		<td align=center>{{.TotalLatency}}</td>
		<td align=left>{{.LatencySummary}}</td>
		</tr>
	{{end}}
	</table>
	{{end}}
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

type debugPageData struct {
	ServerStats
	JSONPath string
}

// debugHTTP 把 Server 包装成调试页面处理器。
type debugHTTP struct {
	*Server
}

// ServeHTTP 渲染调试页面，默认访问路径为 /debug/geerpc。
// 页面内容主要用于观察：当前注册了哪些服务，以及每个方法的调用数、错误数、活跃调用数和耗时。
func (server debugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	err := debug.Execute(w, debugPageData{
		ServerStats: server.Stats(),
		JSONPath:    defaultDebugJSONPath,
	})
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}

// debugJSONHTTP 以 JSON 形式输出服务端统计快照，方便程序化抓取。
type debugJSONHTTP struct {
	*Server
}

// ServeHTTP 返回标准 JSON 指标快照。
func (server debugJSONHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(server.Stats()); err != nil {
		http.Error(w, "rpc: error encoding stats: "+err.Error(), http.StatusInternalServerError)
	}
}
