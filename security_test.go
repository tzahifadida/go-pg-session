package gopgsession

import (
	"encoding/base64"
	"strings"
	"sync"
	"testing"
)

func TestHMACSHA256SignerPool_SignAndVerify(t *testing.T) {
	secret := "supersecretpassword"
	maxPoolSize := 5
	signer := NewHMACSHA256SignerPool([]byte(secret), maxPoolSize)

	message := []byte("Hello, World!")

	signature, err := signer.Sign(message)
	if err != nil {
		t.Fatalf("Failed to sign message: %s", err)
	}

	isValid := signer.Verify(message, signature)
	if !isValid {
		t.Error("Expected the message to be valid")
	}

	// Test with tampered message
	tamperedMessage := []byte("Hello, World?")
	isValid = signer.Verify(tamperedMessage, signature)
	if isValid {
		t.Error("Expected the tampered message to be invalid")
	}
}

func TestHMACSHA256SignerPool_SignAndEncodeVerifyAndDecode(t *testing.T) {
	secret := "supersecretpassword"
	maxPoolSize := 5
	signer := NewHMACSHA256SignerPool([]byte(secret), maxPoolSize)

	message := "Hello, World!"

	signedMessage, err := signer.SignAndEncode(message)
	if err != nil {
		t.Fatalf("Failed to sign and encode message: %s", err)
	}

	isValid, decodedMessage, err := signer.VerifyAndDecode(signedMessage)
	if err != nil {
		t.Fatalf("Failed to verify and decode message: %s", err)
	}

	if !isValid {
		t.Error("Expected the message to be valid")
	}

	if decodedMessage != message {
		t.Errorf("Expected decoded message to be '%s', got '%s'", message, decodedMessage)
	}

	// Test with tampered message
	parts := strings.Split(signedMessage, ".")
	if len(parts) != 2 {
		t.Fatalf("Invalid signed message format")
	}
	encodedMessage, encodedSignature := parts[0], parts[1]
	_, err = base64.StdEncoding.DecodeString(encodedMessage)
	if err != nil {
		t.Fatalf("Failed to verify and decode tampered message: %s", err)
	}
	tamperedMessage := base64.StdEncoding.EncodeToString([]byte("Hello, World?"))
	tamperedSignedMessage := tamperedMessage + "." + encodedSignature

	isValid, _, err = signer.VerifyAndDecode(tamperedSignedMessage)
	if err != nil {
		t.Fatalf("Failed to verify and decode tampered message: %s", err)
	}
	if isValid {
		t.Error("Expected the tampered message to be invalid")
	}
}

func TestHMACSHA256SignerPool_PoolBehavior(t *testing.T) {
	secret := "supersecretpassword"
	maxPoolSize := 1
	signer := NewHMACSHA256SignerPool([]byte(secret), maxPoolSize)

	message := []byte("Hello, World!")

	// First sign should succeed and take the item from the pool
	_, err := signer.Sign(message)
	if err != nil {
		t.Fatalf("Failed to sign message: %s", err)
	}

	// Manually take the HMAC from the pool to simulate pool exhaustion
	_, err = signer.getHMAC()
	if err != nil {
		t.Fatalf("Failed to get HMAC: %s", err)
	}

	// Second sign should still succeed because it creates a new HMAC
	_, err = signer.Sign(message)
	if err != nil {
		t.Fatalf("Expected a new HMAC to be created")
	}
}

func TestHMACSHA256SignerPool_Concurrency(t *testing.T) {
	secret := "supersecretpassword"
	maxPoolSize := 5
	signer := NewHMACSHA256SignerPool([]byte(secret), maxPoolSize)

	var wg sync.WaitGroup
	numGoroutines := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			message := "Hello, World!"
			signedMessage, err := signer.SignAndEncode(message)
			if err != nil {
				t.Errorf("Failed to sign and encode message: %s", err)
				return
			}

			isValid, decodedMessage, err := signer.VerifyAndDecode(signedMessage)
			if err != nil {
				t.Errorf("Failed to verify and decode message: %s", err)
				return
			}

			if !isValid {
				t.Error("Expected the message to be valid")
			}

			if decodedMessage != message {
				t.Errorf("Expected decoded message to be '%s', got '%s'", message, decodedMessage)
			}
		}()
	}

	wg.Wait()
}
