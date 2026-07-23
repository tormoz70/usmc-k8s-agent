package ottlogstrue

import (
	"regexp"
)

// Bucketizer groups log lines into buckets of size N starting at logger-name matches.
type Bucketizer struct {
	size  int
	re    *regexp.Regexp
	buf   []string
	open  bool
}

func NewBucketizer(size int, loggerRegex string) (*Bucketizer, error) {
	if size < 1 {
		size = 2
	}
	re, err := regexp.Compile(loggerRegex)
	if err != nil {
		return nil, err
	}
	return &Bucketizer{size: size, re: re}, nil
}

// Add returns a closed bucket when ready.
func (b *Bucketizer) Add(line string) (bucket []string, closed bool) {
	if b.re.MatchString(line) {
		if b.open && len(b.buf) > 0 {
			out := append([]string(nil), b.buf...)
			b.buf = []string{line}
			b.open = true
			return out, true
		}
		b.buf = []string{line}
		b.open = true
		return nil, false
	}
	if !b.open {
		return nil, false
	}
	b.buf = append(b.buf, line)
	if len(b.buf) >= b.size {
		out := append([]string(nil), b.buf...)
		b.buf = nil
		b.open = false
		return out, true
	}
	return nil, false
}

// Flush returns remaining lines.
func (b *Bucketizer) Flush() []string {
	if len(b.buf) == 0 {
		return nil
	}
	out := append([]string(nil), b.buf...)
	b.buf = nil
	b.open = false
	return out
}
