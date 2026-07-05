package command

// Known command types (extend per phase).
const (
	TypeK8sAPI            = "k8s.api"
	TypeLogsCollect       = "logs.collect"
	TypeWatchSubscribe    = "watch.subscribe"
	TypeWatchUnsubscribe  = "watch.unsubscribe"
	TypeCachePut          = "cache.put"
	TypeCacheDelete       = "cache.delete"
	TypeCacheClear        = "cache.clear"
	TypeLogsStreamStart   = "logs.stream.start"
	TypeLogsStreamStop    = "logs.stream.stop"
	TypeHealthReportStart = "health.report.start"
	TypeHealthReportStop  = "health.report.stop"
)

// SyncCommandTypes are executed sequentially by the sync worker in Phase 1+.
var SyncCommandTypes = map[string]struct{}{
	TypeK8sAPI:            {},
	TypeWatchSubscribe:    {},
	TypeWatchUnsubscribe:  {},
	TypeCachePut:          {},
	TypeCacheDelete:       {},
	TypeCacheClear:        {},
	TypeLogsStreamStart:   {},
	TypeLogsStreamStop:    {},
	TypeHealthReportStart: {},
	TypeHealthReportStop:  {},
}

// AsyncCommandTypes spawn background work (Phase 2+).
var AsyncCommandTypes = map[string]struct{}{
	TypeLogsCollect: {},
}
