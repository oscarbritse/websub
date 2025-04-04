package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// Publish JSON data to a topic, and distributes it to all subscribers
func (h *Hub) PublishContent(w http.ResponseWriter, r *http.Request) {
	// Verify HTTP POST method
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Verify Content-Type is application/json
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "Content-Type must be application/json", http.StatusBadRequest)
		return
	}

	// Read the request body
	content, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate that content is valid JSON
	var jsonData map[string]interface{}
	if err = json.Unmarshal(content, &jsonData); err != nil {
		http.Error(w, "Invalid JSON content: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Extract topic from JSON data
	topicValue, ok := jsonData["topic"]
	if !ok {
		http.Error(w, "JSON must contain 'topic' field", http.StatusBadRequest)
		return
	}

	// Convert topic to string
	topic, ok := topicValue.(string)
	if !ok || topic == "" {
		http.Error(w, "'topic' must be a non-empty string", http.StatusBadRequest)
		return
	}

	// Check if the topic exists
	if !topicExists(topic) {
		http.Error(w, "Topic does not exist", http.StatusNotFound)
		log.Printf("Publish rejected: Topic '%s' does not exist", topic)
		return
	}

	// Only distribute the "content" field provided
	//
	contentValue, ok := jsonData["content"]
	if ok {
		// Convert contentValue back to JSON
		contentBytes, err := json.Marshal(contentValue)
		if err != nil {
			http.Error(w, "Failed to process content field", http.StatusBadRequest)
			return
		}
		content = contentBytes
	}

	// Log for debugging
	log.Printf("Received JSON content from publish endpoint. Topic: %s, Data: %s", topic, content)

	// Distribute JSON content to subscribers asynchronously
	go h.distributeContent(topic, content, "application/json")

	// Return success response
	w.WriteHeader(http.StatusAccepted)

	// Log for debugging
	log.Printf("Accepted JSON content from publish endpoint. Will distribute via topic: %s", topic)
}

// distributeContent sends content to all topic subscribers
func (h *Hub) distributeContent(topic string, content []byte, contentType string) {
	// Use a read lock to safely access the subscriptions map without blocking
	// other goroutines also reading from it. This allows multiple
	// distribution operations to happen concurrently.
	h.mutex.RLock()
	subs, exists := h.subscriptions[topic]
	if !exists {
		h.mutex.RUnlock()
		log.Printf("No subscribers for topic: %s", topic)
		return
	}

	// Creating a copy allows us to release the lock quickly while still having
	// access to all subscribers. This prevents holding the lock during the potentially
	// slow network operations that follow.
	subscriptionsCopy := make([]Subscription, len(subs))
	copy(subscriptionsCopy, subs)
	h.mutex.RUnlock()

	log.Printf("Distributing content to %d subscriber(s) via topic: %s", len(subscriptionsCopy), topic)

	// Process each subscription in parallel using goroutines
	for _, sub := range subscriptionsCopy {
		// Launch a separate goroutine for each delivery to prevent blocking
		go deliverContentToSubscriber(sub, topic, content, contentType)
	}
}

// deliverContentToSubscriber sends content to an individual subscriber
// This function handles the actual HTTP request creation, signing, and delivery
// It's designed to run in its own goroutine to allow concurrent delivery
func deliverContentToSubscriber(sub Subscription, topic string, content []byte, contentType string) {
	// Set up a HTTP POST request to the subscriber's callback URL
	// including the content in the request body
	req, err := http.NewRequest("POST", sub.Callback, bytes.NewReader(content))
	if err != nil {
		log.Printf("Failed to create request: %s", err)
		return
	}

	// Add required headers according to the WebSub specification
	// Content-Type should match the original content
	// Link headers must identify the topic and hub URLs
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Link", fmt.Sprintf(`<%s>; rel="self", <%s>; rel="hub"`,
		topic, "http://hub:8080"))

	// Generate and add signature if a secret was provided during subscription
	// This allows subscribers to verify the content came from the hub
	//
	// 8.3 Distribution
	// The Hub MUST use the exact callback used by the subscriber (including the use of HTTPS).
	// Hubs MUST sign their requests using the hub.secret supplied by subscribers if requested.
	if sub.Secret != "" {
		signature := generateSignature(content, sub.Secret)
		signatureHeader := fmt.Sprintf("sha256=%s", signature)
		req.Header.Set("X-Hub-Signature", signatureHeader)

		// Log signature details for debugging verification issues
		// contentPreview := string(content)
		// if len(contentPreview) > 50 {
		//     contentPreview = contentPreview[:50] + "..." // Truncate long content
		// }
		// log.Printf("  Adding signature for %s:", sub.Callback)
		// log.Printf("    Content (%d bytes): %s", len(content), contentPreview)
		// log.Printf("    X-Hub-Signature: %s", signatureHeader)
	}

	// Send the request with timeout
	// Use an HTTP client with timeout to prevent hanging if a subscriber
	// is unresponsive.
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to deliver content to %s: %s", sub.Callback, err)
		return
	}
	// Ensure connection is always closed even if an error occurs later
	defer resp.Body.Close()

	// Log delivery result for monitoring and debugging purposes
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("Successfully delivered content. Callback: %s, Status: %d",
			sub.Callback, resp.StatusCode)
	} else {
		log.Printf("Warning: Subscriber %s returned error status: %d",
			sub.Callback, resp.StatusCode)
	}
}
