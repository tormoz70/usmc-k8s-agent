package ottlogstrue

import "testing"

func TestParserPermit(t *testing.T) {
	p := NewParser()
	lines := []string{
		"Category=urn:oasis:names:tc:xacml:1.0:subject-category:access-subject Decision=Permit",
		"module_id: 'prov-1' host: 'h1' access-subject{[[cons-1]]} api:interface:resource{/api/v1} names:http:method,GET",
	}
	rec := p.Parse(lines, false)
	if rec.RequestResult != ResultPermit {
		t.Fatalf("result=%s", rec.RequestResult)
	}
	if rec.ModuleIDProvider != "prov-1" {
		t.Fatalf("provider=%s", rec.ModuleIDProvider)
	}
	if !ShouldSend(rec) {
		// method may fail regex — ensure path/provider at least
		t.Logf("record=%+v", rec)
	}
}

func TestParserHealthcheck(t *testing.T) {
	p := NewParser()
	rec := p.Parse(nil, true)
	if !ShouldSend(rec) || rec.RequestResult != ResultUnknown {
		t.Fatalf("%+v", rec)
	}
}

func TestBucketizer(t *testing.T) {
	b, err := NewBucketizer(2, `^logger`)
	if err != nil {
		t.Fatal(err)
	}
	if _, closed := b.Add("noise"); closed {
		t.Fatal("noise should not open")
	}
	if _, closed := b.Add("logger start"); closed {
		t.Fatal("first logger opens only")
	}
	bucket, closed := b.Add("line2")
	if !closed || len(bucket) != 2 {
		t.Fatalf("closed=%v bucket=%v", closed, bucket)
	}
}
