// Package gopgsession provides utilities for session management and security.

package gopgsession

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"hash"
)

// HMACSHA256SignerPool represents a limited pool of HMAC-SHA256 signers.
// It provides thread-safe access to a pool of HMAC signers for efficient signing and verification.
type HMACSHA256SignerPool struct {
	pool    chan hash.Hash
	secret  []byte
	maxSize int
}

// NewHMACSHA256SignerPool initializes a new limited pool of HMAC-SHA256 signers.
//
// Parameters:
//   - secret: A string containing the secret key used for HMAC operations.
//   - maxPoolSize: An integer specifying the maximum number of HMAC signers to keep in the pool.
//
// Returns:
//   - A pointer to a new HMACSHA256SignerPool instance.
func NewHMACSHA256SignerPool(secret string, maxPoolSize int) *HMACSHA256SignerPool {
	return &HMACSHA256SignerPool{
		secret:  []byte(secret),
		maxSize: maxPoolSize,
		pool:    make(chan hash.Hash, maxPoolSize),
	}
}

// getHMAC retrieves or creates an HMAC from the pool.
//
// Returns:
//   - A hash.Hash interface representing an HMAC-SHA256 instance.
//   - An error if the HMAC creation fails (which is unlikely and would indicate a system-level issue).
func (s *HMACSHA256SignerPool) getHMAC() (hash.Hash, error) {
	select {
	case h := <-s.pool:
		return h, nil
	default:
		return hmac.New(sha256.New, s.secret), nil
	}
}

// releaseHMAC releases an HMAC back to the pool.
//
// Parameters:
//   - h: A hash.Hash interface representing the HMAC-SHA256 instance to be released back to the pool.
func (s *HMACSHA256SignerPool) releaseHMAC(h hash.Hash) {
	h.Reset()
	select {
	case s.pool <- h:
	default:
		// Pool is full, discard the HMAC
	}
}

// Sign signs a message using a pooled HMAC.
//
// Parameters:
//   - message: A string containing the message to be signed.
//
// Returns:
//   - A string containing the base64-encoded HMAC signature.
//   - An error if the signing process fails.
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

// Verify verifies a signed message using a pooled HMAC and returns the original message.
//
// Parameters:
//   - message: A string containing the original message that was signed.
//   - signature: A string containing the base64-encoded HMAC signature to verify.
//
// Returns:
//   - A boolean indicating whether the signature is valid (true) or not (false).
//   - A string containing the original message (unchanged from the input).
//   - An error if the verification process fails (e.g., due to invalid base64 encoding).
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
