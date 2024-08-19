// Package gopgsession provides utilities for session management and security.

package gopgsession

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"hash"
	"strings"
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
//   - secret: A byte slice containing the secret key used for HMAC operations.
//   - maxPoolSize: An integer specifying the maximum number of HMAC signers to keep in the pool.
//
// Returns:
//   - A pointer to a new HMACSHA256SignerPool instance.
func NewHMACSHA256SignerPool(secret []byte, maxPoolSize int) *HMACSHA256SignerPool {
	return &HMACSHA256SignerPool{
		secret:  secret,
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
//   - message: A byte slice containing the message to be signed.
//
// Returns:
//   - A byte slice containing the HMAC signature.
//   - An error if the signing process fails.
func (s *HMACSHA256SignerPool) Sign(message []byte) ([]byte, error) {
	h, err := s.getHMAC()
	if err != nil {
		return nil, err
	}
	defer s.releaseHMAC(h)

	h.Write(message)
	return h.Sum(nil), nil
}

// Verify verifies a signed message using a pooled HMAC.
//
// Parameters:
//   - message: A byte slice containing the original message that was signed.
//   - signature: A byte slice containing the HMAC signature to verify.
//
// Returns:
//   - A boolean indicating whether the signature is valid (true) or not (false).
func (s *HMACSHA256SignerPool) Verify(message, signature []byte) bool {
	h, err := s.getHMAC()
	if err != nil {
		return false
	}
	defer s.releaseHMAC(h)

	h.Write(message)
	expectedSignature := h.Sum(nil)

	return hmac.Equal(expectedSignature, signature)
}

// SignAndEncode signs a message and encodes it with the signature.
//
// Parameters:
//   - message: A string containing the message to be signed.
//
// Returns:
//   - A string containing the base64-encoded message and HMAC signature, separated by a delimiter.
//   - An error if the signing process fails.
func (s *HMACSHA256SignerPool) SignAndEncode(message string) (string, error) {
	signature, err := s.Sign([]byte(message))
	if err != nil {
		return "", err
	}

	// Encode message and signature separately
	encodedMessage := base64.StdEncoding.EncodeToString([]byte(message))
	encodedSignature := base64.StdEncoding.EncodeToString(signature)

	// Combine encoded message and signature
	return encodedMessage + "." + encodedSignature, nil
}

// VerifyAndDecode verifies a signed and encoded message and returns the original message.
//
// Parameters:
//   - signedMessage: A string containing the base64-encoded message and HMAC signature.
//
// Returns:
//   - A boolean indicating whether the signature is valid (true) or not (false).
//   - A string containing the original message.
//   - An error if the verification process fails.
func (s *HMACSHA256SignerPool) VerifyAndDecode(signedMessage string) (bool, string, error) {
	// Split the combined message and signature
	parts := strings.Split(signedMessage, ".")
	if len(parts) != 2 {
		return false, "", errors.New("invalid signed message format")
	}

	encodedMessage, encodedSignature := parts[0], parts[1]

	// Decode the message
	messageBytes, err := base64.StdEncoding.DecodeString(encodedMessage)
	if err != nil {
		return false, "", err
	}

	// Decode the signature
	signatureBytes, err := base64.StdEncoding.DecodeString(encodedSignature)
	if err != nil {
		return false, "", err
	}

	// Verify the signature
	isValid := s.Verify(messageBytes, signatureBytes)

	return isValid, string(messageBytes), nil
}
