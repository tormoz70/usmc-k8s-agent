package ottlogstrue

import (
	"regexp"
	"strings"
)

const (
	ResultPermit  = "PERMIT"
	ResultDeny    = "DENY"
	ResultUnknown = "UNKNOWN"

	xacmlCategory = "Category=urn:oasis:names:tc:xacml:1.0:subject-category:access-subject"
)

var (
	rePermit = regexp.MustCompile(`(Result: 'Result\()|(Decision=Permit)`)
	reDeny   = regexp.MustCompile(`(Result: 'Deny')|('Deny')|(Decision=Deny)`)
	reModID  = regexp.MustCompile(`module_id:\s*'([^']*)'`)
	reHost   = regexp.MustCompile(`host:\s*'([^']*)'`)
	rePath   = regexp.MustCompile(`api:interface:resource\{([^}]*)\}`)
	reMethod = regexp.MustCompile(`names:http:method[^\w]*([A-Z]+)`)
	reCons   = regexp.MustCompile(`access-subject\{\[\[([^\]]*)\]\]\}`)
	reProv   = regexp.MustCompile(`recipient-subject\{\[\[([^\]]*)\]\]\}`)
)

// Record is a parsed XACML / healthcheck event.
type Record struct {
	RequestResult    string
	RequestPath      string
	RequestMethod    string
	ModuleIDProvider string
	ModuleIDConsumer string
	Host             string
	Healthcheck      bool
	Received         int64
}

// Parser resolves V01 vs DEFAULT and extracts fields.
type Parser struct{}

func NewParser() *Parser { return &Parser{} }

func (p *Parser) Parse(lines []string, healthcheck bool) Record {
	if healthcheck {
		return Record{RequestResult: ResultUnknown, Healthcheck: true}
	}
	joined := strings.Join(lines, " ")
	if !strings.Contains(joined, xacmlCategory) {
		return Record{RequestResult: ResultUnknown}
	}
	rec := Record{}
	switch {
	case rePermit.MatchString(joined):
		rec.RequestResult = ResultPermit
	case reDeny.MatchString(joined):
		rec.RequestResult = ResultDeny
	default:
		rec.RequestResult = ResultUnknown
	}
	if m := reModID.FindStringSubmatch(joined); len(m) > 1 {
		rec.ModuleIDProvider = m[1]
	}
	if m := reProv.FindStringSubmatch(joined); len(m) > 1 && rec.ModuleIDProvider == "" {
		rec.ModuleIDProvider = m[1]
	}
	if m := reHost.FindStringSubmatch(joined); len(m) > 1 {
		rec.Host = m[1]
	}
	if m := reCons.FindStringSubmatch(joined); len(m) > 1 {
		rec.ModuleIDConsumer = m[1]
	}
	if m := rePath.FindStringSubmatch(joined); len(m) > 1 {
		rec.RequestPath = m[1]
	}
	if m := reMethod.FindStringSubmatch(joined); len(m) > 1 {
		rec.RequestMethod = m[1]
	}
	return rec
}

// ShouldSend applies the Java send gate: healthcheck OR (provider+method+consumer+path).
func ShouldSend(r Record) bool {
	if r.Healthcheck {
		return true
	}
	return r.ModuleIDProvider != "" && r.RequestMethod != "" && r.ModuleIDConsumer != "" && r.RequestPath != ""
}
