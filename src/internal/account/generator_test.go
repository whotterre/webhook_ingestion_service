package account

import (
	"strings"
	"testing"
)

func TestWemaPrefixAccountNumberFormatting(t *testing.T) {
	testCases := []struct {
		prefix   string
		seq      int64
		expected string
	}{
		{prefix: "6930", seq: 1, expected: "6930000001"},
		{prefix: "6930", seq: 42, expected: "6930000042"},
		{prefix: "6930", seq: 123456, expected: "6930123456"},
		{prefix: "6930", seq: 999999, expected: "6930999999"},
		{prefix: "735", seq: 5, expected: "7350000005"},
	}

	for _, tc := range testCases {
		accNum := FormatAccountNumber(tc.prefix, tc.seq)
		if len(accNum) != 10 {
			t.Errorf("expected exactly 10 digits for prefix %s seq %d, got %d (%s)", tc.prefix, tc.seq, len(accNum), accNum)
		}
		if accNum != tc.expected {
			t.Errorf("expected %s, got %s", tc.expected, accNum)
		}
		if !strings.HasPrefix(accNum, tc.prefix) {
			t.Errorf("expected prefix %s in %s", tc.prefix, accNum)
		}
	}
}

func TestNewRandomToken(t *testing.T) {
	t1 := newRandomToken()
	t2 := newRandomToken()

	if t1 == "" || t2 == "" {
		t.Fatalf("expected non-empty tokens")
	}
	if t1 == t2 {
		t.Fatalf("tokens should be unique, got duplicate: %s", t1)
	}
}
