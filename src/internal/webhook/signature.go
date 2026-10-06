package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

var (
	ErrMissingSignature  = errors.New("missing signature header")
	ErrMalformedSignature = errors.New("malformed signature header")
	ErrStaleSignature    = errors.New("signature timestamp too old")
	ErrSignatureMismatch = errors.New("signature mismatch")
)

func verifySignature(secret string, body []byte, header string) error {
	maxSkew := 	5 * time.Minute
	if header == "" {
		return ErrMissingSignature
	}

	ts, sig, err := parseSignatureHeader(header)
	if err != nil {
		return err
	}

	// Timestamp window: reject anything older than maxSkew.
	if time.Since(time.Unix(ts, 0)) > maxSkew {
		return ErrStaleSignature
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	if subtle.ConstantTimeCompare([]byte(expected), []byte(sig)) != 1 {
		return ErrSignatureMismatch
	}
	return nil
}

func parseSignatureHeader(header string) (int64, string, error) {
	var tsStr, sig string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return 0, "", ErrMalformedSignature
		}
		switch k {
		case "t":
			tsStr = v
		case "v1":
			sig = v
		}
	}
	if tsStr == "" || sig == "" {
		return 0, "", ErrMalformedSignature
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return 0, "", ErrMalformedSignature
	}
	return ts, sig, nil
}