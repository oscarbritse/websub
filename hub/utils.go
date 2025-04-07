package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net/url"
)

// Check if a topic is valid
func topicExists(topic string) bool {
	// Predefined list of valid topics
	validTopics := []string{
		"/a/topic", // the topic we are interested in
		"/another/topic",
		"/yet/another/topic",
	}

	// Check against the valid topics
	for _, validTopic := range validTopics {
		if topic == validTopic {
			return true
		}
	}

	// Log what topic was requested vs what's available
	log.Printf("Topic validation failed: requested '%s', valid topics are: %v",
		topic, validTopics)

	// Topic does not exist
	return false
}

// Create a random string for subscriber intent verification
// 16 bytes = 128 bits of randomness (strong security)
// crypto/rand package (cryptographically secure)
// results in a 32-character string
func generateChallenge() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Create an HMAC-SHA256 signature for content verification
// Creates a new HMAC-SHA256 hash using the provided secret as the key
// Writes the content bytes to the hash
// Calculates the final hash value and encodes it as a hexadecimal string
// This signature is sent in the X-Hub-Signature header when distributing content
// and allows subscribers to verify the content came from the hub they subscribed with
func generateSignature(content []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(content)
	return hex.EncodeToString(mac.Sum(nil))
}

// Print form values for debugging
func debugFormValues(prefix string, form url.Values) {
	fmt.Println(prefix)
	fmt.Println("-------------------------------------")
	fmt.Println("All form values:")
	for key, values := range form {
		fmt.Printf("%s: %v\n", key, values)
	}
	fmt.Println("-------------------------------------")
}
