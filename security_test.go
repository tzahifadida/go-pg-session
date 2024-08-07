package gopgsession

import (
	"sync"
	"testing"
)

func TestHMACSHA256SignerPool_SignAndVerify(t *testing.T) {
	secret := "supersecretpassword"
	maxPoolSize := 5
	signer := NewHMACSHA256SignerPool(secret, maxPoolSize)

	message := "Hello, World!"

	signedMessage, err := signer.Sign(message)
	if err != nil {
		t.Fatalf("Failed to sign message: %s", err)
	}

	isValid, originalMessage, err := signer.Verify(message, signedMessage)
	if err != nil {
		t.Fatalf("Failed to verify message: %s", err)
	}

	if !isValid {
		t.Error("Expected the message to be valid")
	}

	if originalMessage != message {
		t.Errorf("Expected original message to be '%s', got '%s'", message, originalMessage)
	}
}

func TestHMACSHA256SignerPool_PoolBehavior(t *testing.T) {
	secret := "supersecretpassword"
	maxPoolSize := 1
	signer := NewHMACSHA256SignerPool(secret, maxPoolSize)

	message := "Hello, World!"

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
	signer := NewHMACSHA256SignerPool(secret, maxPoolSize)

	var wg sync.WaitGroup
	numGoroutines := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			message := "Hello, World!"
			signedMessage, err := signer.Sign(message)
			if err != nil {
				t.Errorf("Failed to sign message: %s", err)
				return
			}

			isValid, originalMessage, err := signer.Verify(message, signedMessage)
			if err != nil {
				t.Errorf("Failed to verify message: %s", err)
				return
			}

			if !isValid {
				t.Error("Expected the message to be valid")
			}

			if originalMessage != message {
				t.Errorf("Expected original message to be '%s', got '%s'", message, originalMessage)
			}
		}()
	}

	wg.Wait()
}
