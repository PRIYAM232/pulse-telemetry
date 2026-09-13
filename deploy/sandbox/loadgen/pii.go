package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// All PII here is synthetic and invalid by construction:
//   - Emails use the RFC 2606 reserved domains (example.com/.org/.net).
//   - SSNs use area numbers 900-999, which the SSA never issues, with group
//     numbers 01-49, outside every IRS ITIN range.
//   - Card numbers are the card networks' published test PANs.

// RedactPattern is the REDACT pattern the sandbox runbook has operators paste
// into the dashboard: one RE2 alternation over SSNs, 13-16 digit card
// numbers (bare, spaced, or dashed), and email addresses. It also compiles as
// a JS RegExp, which the dashboard's form validation requires. pii_test.go
// proves it masks everything this generator emits and nothing in the noise
// stream, so the runbook and the generator cannot drift apart.
const RedactPattern = `\b\d{3}-\d{2}-\d{4}\b|\b(?:\d[ -]?){12,15}\d\b|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`

var (
	customerNames = []string{
		"jane.doe", "john.smith", "maria.garcia", "wei.chen",
		"aisha.khan", "liam.oconnor", "sofia.rossi", "kenji.sato",
	}
	emailDomains = []string{"example.com", "example.org", "example.net"}

	testCards = []string{
		"4111111111111111", // Visa
		"4242424242424242", // Visa
		"5555555555554444", // Mastercard
		"2223003122003222", // Mastercard 2-series
		"6011111111111117", // Discover
		"378282246310005",  // American Express
	}
)

// piiSource draws synthetic PII from one seeded RNG, so tests are
// reproducible and the generator needs no locking (single goroutine).
type piiSource struct{ rnd *rand.Rand }

func (p piiSource) email() string {
	return customerNames[p.rnd.IntN(len(customerNames))] + "@" + emailDomains[p.rnd.IntN(len(emailDomains))]
}

func (p piiSource) ssn() string {
	return fmt.Sprintf("9%02d-%02d-%04d", p.rnd.IntN(100), 1+p.rnd.IntN(49), 1+p.rnd.IntN(9999))
}

// card returns a test PAN in one of the three shapes real logs leak them in:
// bare, space-grouped, or dash-grouped (4-4-4-4, or 4-6-5 for Amex).
func (p piiSource) card() string {
	pan := testCards[p.rnd.IntN(len(testCards))]
	sep := []string{"", " ", "-"}[p.rnd.IntN(3)]
	if sep == "" {
		return pan
	}
	groups := []int{4, 4, 4, 4}
	if len(pan) == 15 {
		groups = []int{4, 6, 5}
	}
	parts := make([]string, 0, len(groups))
	for _, n := range groups {
		parts = append(parts, pan[:n])
		pan = pan[n:]
	}
	return strings.Join(parts, sep)
}

// paymentEvent is one well-behaved tenant's log line: an event name, a body
// with PII embedded the way application code carelessly logs it, and the
// customer email that also goes out as the user.email attribute.
type paymentEvent struct {
	name, body, email string
}

func (p piiSource) paymentEvent() paymentEvent {
	email := p.email()
	switch p.rnd.IntN(4) {
	case 0:
		return paymentEvent{"payment.authorized", fmt.Sprintf(
			"payment authorized customer=%s card=%s amount=%.2f currency=USD",
			email, p.card(), 5+p.rnd.Float64()*495), email}
	case 1:
		return paymentEvent{"kyc.verified", fmt.Sprintf(
			"kyc verification passed customer=%s ssn=%s", email, p.ssn()), email}
	case 2:
		return paymentEvent{"refund.issued", fmt.Sprintf(
			"refund issued card=%s order=ord_%06x reason=duplicate_charge",
			p.card(), p.rnd.IntN(1<<24)), email}
	default:
		return paymentEvent{"password_reset.sent", fmt.Sprintf(
			"password reset link sent to %s", email), email}
	}
}

// noiseBody is the noisy neighbor's DEBUG line. PII-free on purpose, and
// no digit run longer than 8, so no REDACT pattern ever touches it and the
// THROTTLE effect can be measured on its own.
func (p piiSource) noiseBody() string {
	return fmt.Sprintf("cache miss key=idx:%08x shard=%d latency_ms=%d attempt=%d",
		p.rnd.Uint32(), p.rnd.IntN(64), p.rnd.IntN(900), 1+p.rnd.IntN(3))
}

// checkoutQueries are the db.query.text values on a checkout trace's two
// database spans — the classic "PII in a SQL statement" leak.
func (p piiSource) checkoutQueries(email string) (lookup, insert string) {
	lookup = fmt.Sprintf("SELECT id, name FROM customers WHERE email = '%s' AND ssn = '%s'", email, p.ssn())
	insert = fmt.Sprintf("INSERT INTO payments (customer_email, card_number, amount) VALUES ('%s', '%s', %.2f)",
		email, p.card(), 5+p.rnd.Float64()*495)
	return lookup, insert
}
