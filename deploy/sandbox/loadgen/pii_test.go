package main

import (
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"
)

// The data plane masks REGEX_MATCH hits with ReplaceAllLiteralString and this
// placeholder (filterprocessor/rules.go); the tests apply the same operation.
const maskPlaceholder = "[REDACTED_PATTERN]"

var redactRE = regexp.MustCompile(RedactPattern)

func testSource() piiSource { return piiSource{rand.New(rand.NewPCG(1, 2))} }

func TestRedactPatternMasksEveryPIIShape(t *testing.T) {
	p := testSource()
	for i := 0; i < 5000; i++ {
		for _, v := range []string{p.email(), p.ssn(), p.card()} {
			if got := redactRE.ReplaceAllLiteralString(v, maskPlaceholder); got != maskPlaceholder {
				t.Fatalf("%q masked to %q, want the whole value replaced", v, got)
			}
		}
	}
}

// Bodies and queries must come out with nothing PII-shaped left over. The
// oracle is independent of RedactPattern: with separators stripped, SSNs and
// cards are 9+ digit runs (order IDs and amounts never are), and every email
// has an @.
func TestRedactPatternScrubsPayloads(t *testing.T) {
	leftover := regexp.MustCompile(`\d{9,}|@`)
	stripSeparators := strings.NewReplacer(" ", "", "-", "")
	p := testSource()
	for i := 0; i < 5000; i++ {
		ev := p.paymentEvent()
		lookup, insert := p.checkoutQueries(ev.email)
		for _, s := range []string{ev.body, lookup, insert} {
			masked := redactRE.ReplaceAllLiteralString(s, maskPlaceholder)
			if !strings.Contains(masked, maskPlaceholder) {
				t.Fatalf("%q: nothing masked", s)
			}
			if m := leftover.FindString(stripSeparators.Replace(masked)); m != "" {
				t.Fatalf("%q masked to %q, still contains %q", s, masked, m)
			}
		}
	}
}

func TestRedactPatternSparesNoise(t *testing.T) {
	p := testSource()
	for i := 0; i < 5000; i++ {
		if b := p.noiseBody(); redactRE.MatchString(b) {
			t.Fatalf("noise body %q matches the PII pattern", b)
		}
	}
}
