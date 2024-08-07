package gopgsession

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"hash"
)

// HMACSHA256SignerPool represents a limited pool of HMAC-SHA256 signers
type HMACSHA256SignerPool struct {
	pool    chan hash.Hash
	secret  []byte
	maxSize int
}

// NewHMACSHA256SignerPool initializes a new limited pool of HMAC-SHA256 signers
func NewHMACSHA256SignerPool(secret string, maxPoolSize int) *HMACSHA256SignerPool {
	return &HMACSHA256SignerPool{
		secret:  []byte(secret),
		maxSize: maxPoolSize,
		pool:    make(chan hash.Hash, maxPoolSize),
	}
}

// getHMAC retrieves or creates an HMAC from the pool
func (s *HMACSHA256SignerPool) getHMAC() (hash.Hash, error) {
	select {
	case h := <-s.pool:
		return h, nil
	default:
		return hmac.New(sha256.New, s.secret), nil
	}
}

// releaseHMAC releases an HMAC back to the pool
func (s *HMACSHA256SignerPool) releaseHMAC(h hash.Hash) {
	h.Reset()
	select {
	case s.pool <- h:
	default:
		// Pool is full, discard the HMAC
	}
}

// Sign signs a message using a pooled HMAC
func (s *HMACSHA256SignerPool) Sign(message string) (string, error) {
	h, err := s.getHMAC()
	if err != nil {
		return "", err
	}
	defer s.releaseHMAC(h)

	h.Write([]byte(message))
	signature := h.Sum(nil)
	return base64.StdEncoding.EncodeToString(signature), nil
}

// Verify verifies a signed message using a pooled HMAC and returns the original message
func (s *HMACSHA256SignerPool) Verify(message, signature string) (bool, string, error) {
	h, err := s.getHMAC()
	if err != nil {
		return false, "", err
	}
	defer s.releaseHMAC(h)

	expectedSignature, err := s.Sign(message)
	if err != nil {
		return false, "", err
	}

	isValid := hmac.Equal([]byte(expectedSignature), []byte(signature))
	return isValid, message, nil
}
