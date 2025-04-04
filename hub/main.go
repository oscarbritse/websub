package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

// Store topic-to-subscriptions mappings
//
// Subscriber -> Hub
// It tries to subscribe to a topic
// It generates a random callback endpoint when it tries to subscribe
// It generates a random secret when it tries to subscribe
//
// Struct is similar to a Python dataclass
type Subscription struct {
	Callback string
	Topic    string
	Secret   string
}

// Map is similar to a Python dict
type Hub struct {
	subscriptions map[string][]Subscription
	mutex         sync.RWMutex // Protects the subscriptions map
}

//	subscriptions: {
//	    "/a/topic": [
//	        {
//	            Callback: "https://subscriber1.com/callback",
//	            Topic: "/a/topic",
//	            Secret: "secret-123"
//	        },
//	        {
//	            Callback: "https://subscriber2.com/callback",
//	            Topic: "/a/topic",
//	            Secret: "secret-456"
//	        }
//	    ]
//	}
//

// NewHub creates a new WebSub hub
//
// Pointers: The * and & symbols deal with memory addresses:
// *Hub means "a pointer to a Hub"
// &hub means "the memory address of hub"
// This helps avoid unnecessary copying of data
//
// make is a built-in function in Go that creates slices, maps, and channels
// make is similar to the dict() function in Python
func NewHub() *Hub {
	// Create a new Hub instance
	// The map is initialized with an empty map
	hub := Hub{subscriptions: make(map[string][]Subscription)}

	return &hub
}

// Subscribe handles WebSub subscription requests (Subscriber -> Hub)
// Subscriber makes a POST to the hub to subscribe to updates about topic
//
// A function which takes a "receiver" (argument before func name) is called a method in Go
// The receiver is a special parameter that allows you to call the method on the struct
// The receiver is similar to the self parameter in Python
//
// ResponseWriter is an interface that allows you to write HTTP responses
// (modify in-place rather than return-based approach)
// Request is a struct that represents an HTTP request
func (hub *Hub) Subscribe(writer http.ResponseWriter, request *http.Request) {

	// Fail fast philosophy. return exits the function immediately

	// Log request details for debugging
	log.Printf("Received subscription request from %s", request.RemoteAddr)

	// Verify HTTP method is POST
	if request.Method != "POST" {
		errorMsg := "Method not allowed. WebSub requires HTTP POST."
		log.Println(errorMsg)
		http.Error(writer, errorMsg, http.StatusMethodNotAllowed)
		return
	}

	// Verify Content-Type is application/x-www-form-urlencoded
	contentType := request.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/x-www-form-urlencoded") {
		errorMsg := "Invalid Content-Type. WebSub requires application/x-www-form-urlencoded."
		log.Println(errorMsg)
		http.Error(writer, errorMsg, http.StatusBadRequest)
		return
	}

	// Parse form data (implicitly checks UTF-8 encoding)
	err := request.ParseForm()
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to parse form data: %v. WebSub requires UTF-8 encoding.", err)
		log.Println(errorMsg)
		http.Error(writer, errorMsg, http.StatusBadRequest)
		return
	}

	// Print form values for debugging
	// DebugFormValues("WebSub subscription request received:", request.Form)

	// https://www.w3.org/TR/websub/#hubs
	// A conforming hub:
	// 	* MUST accept a subscription request with the parameters hub.callback, hub.mode and hub.topic.
	// 	* MUST accept a subscription request with a hub.secret parameter.
	// 	* MAY respect the requested lease duration in subscription requests. (not included from websub-client)
	callback := request.Form.Get("hub.callback")
	mode := request.Form.Get("hub.mode")
	topic := request.Form.Get("hub.topic")
	secret := request.Form.Get("hub.secret")

	// Validate form data
	// OR (||), AND (&&), neither subscribe nor unsubscribe
	// Check that we have all required parameters according to WebSub specification
	if callback == "" || topic == "" || (mode != "subscribe" && mode != "unsubscribe") {
		// Build a helpful error message
		errorMsg := "WebSub subscription request error: "

		// Check each required parameter
		if callback == "" {
			errorMsg += "Missing hub.callback parameter. "
		}

		if topic == "" {
			errorMsg += "Missing hub.topic parameter. "
		}

		if mode == "" {
			errorMsg += "Missing hub.mode parameter. "
		} else if mode != "subscribe" && mode != "unsubscribe" {
			errorMsg += fmt.Sprintf("Invalid hub.mode value: '%s'. Must be 'subscribe' or 'unsubscribe'. ", mode)
		}

		// Log the error for debugging
		log.Println(errorMsg)

		// Send error response to client
		http.Error(writer, errorMsg, http.StatusBadRequest)
		return
	}

	// Send 202 Accepted respons
	// 5.1.2 Subscription Response Details
	// If the hub URL supports WebSub and is able to handle the subscription or unsubscription request,
	// it MUST respond to a subscription request with an HTTP [RFC7231] 202 "Accepted" response to
	// indicate that the request was received and will now be verified (Section 4.3 ) and validated
	// (Section 4.2 ) by the hub
	writer.WriteHeader(http.StatusAccepted)

	// Used for debugging
	if mode == "subscribe" {
		log.Printf("Subscription request for topic '%s' with callback '%s'", topic, callback)
		if secret != "" {
			log.Printf("Secret provided for subscription")
		} else {
			log.Printf("No secret provided for subscription")
		}
	} else if mode == "unsubscribe" {
		log.Printf("Unsubscribe request for topic '%s' with callback '%s'", topic, callback)
	}

	// Verification
	// Verification happens asynchronously after sending 202 response
	// 5.1.2 Subscription Response Details
	// The hub SHOULD perform the verification and validation of intent as soon as possible.
	go func() {

		// Validation logic - example checks that could lead to denial:
		//Topic doesn't exist
		if !topicExists(topic) {
			hub.denySubscription(callback, topic, "Topic does not exist")
			return
		}

		// 8.2 Subscriptions
		// When performing intent verification, the hub SHOULD use a random, single-use hub.challenge.
		// Generate a random challenge string
		challenge := generateChallenge()

		// Verify intent by sending a GET request to the callback URL
		// Confirms the subscription request came from someone who controls the callback URL
		// Prevents attackers from subscribing someone else's URL to a topic
		intentVerified := hub.verifyIntent(callback, mode, topic, challenge)

		// Process verification result internally (don't send another HTTP response)
		if intentVerified {
			// Add or remove the subscription based on mode
			if mode == "subscribe" {
				// Add subscription
				hub.addSubscription(callback, topic, secret)
				log.Printf("Verified and added subscription: %s for topic: %s", callback, topic)
			} else {
				hub.removeSubscription(callback, topic)
				log.Printf("Verified and removed subscription: %s for topic: %s", callback, topic)
			}
		} else {
			log.Printf("Failed to verify intent for %s request: %s, topic: %s",
				mode, callback, topic)
		}
	}()

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

// denySubscription sends a denial notification to the subscriber
//
// 5.2 Subscription Validation
// If (and when) the subscription is denied, the hub MUST inform the subscriber
// by sending an HTTP [RFC7231] (or HTTPS [RFC2818]) GET request to the subscriber's
// callback URL as given in the subscription request.
func (hub *Hub) denySubscription(callback, topic, reason string) {
	// Build denial URL with query parameters
	callbackURL, err := url.Parse(callback)
	if err != nil {
		log.Printf("Invalid callback URL for denial: %s", err)
		return
	}

	// Construct the callback query with denial parameters
	callbackQuery := callbackURL.Query()
	callbackQuery.Add("hub.mode", "denied")
	callbackQuery.Add("hub.topic", topic)
	if reason != "" {
		callbackQuery.Add("hub.reason", reason)
	}
	callbackURL.RawQuery = callbackQuery.Encode()

	// Send GET request to notify subscriber of denial
	resp, err := http.Get(callbackURL.String())
	if err != nil {
		log.Printf("Failed to send denial notification: %s", err)
		return
	}
	// Cleanup happens when function returns regardless of how it finishes
	// A bit like finally() in Python
	defer resp.Body.Close()

	log.Printf("Sent subscription denial for topic '%s' to '%s'. Reason: %s",
		topic, callback, reason)
}

// topicExists checks if a topic is valid
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

// verifyIntent sends a verification request to the subscriber
func (hub *Hub) verifyIntent(callback, mode, topic, challenge string) bool {

	// Build verification URL with query parameters
	callbackURL, err := url.Parse(callback)
	if err != nil {
		log.Printf("Invalid callback URL: %s", err)
		return false
	}

	log.Printf("Verifying intent: URL=%s, mode=%s, topic=%s, challenge=%s",
		callbackURL.String(), mode, topic, challenge)

	// Construct the callback query with parameters
	callbackQuery := callbackURL.Query()
	callbackQuery.Add("hub.mode", mode)
	callbackQuery.Add("hub.topic", topic)
	callbackQuery.Add("hub.challenge", challenge)
	callbackURL.RawQuery = callbackQuery.Encode()

	// Send GET request to callback URL of subscriber (Hub -> Subscriber)
	// Hub verifies the subscription attempt with a GET
	resp, err := http.Get(callbackURL.String())
	if err != nil {
		log.Printf("Failed to verify intent: %s", err)
		return false
	}
	// Cleanup happens when function returns regardless of how it finishes
	// A bit like finally() in Python
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read verification response: %s", err)
		return false
	}

	// Verify HTTP 200 response and body matches challenge
	// Verify that the subscriber's response has an HTTP 200 OK status code
	// Remove any whitespace. Convert the byte array to a string
	// Compares the trimmed response body exactly with our original challenge string
	// Confirms the subscriber can both receive and respond correctly on that URL
	return resp.StatusCode == http.StatusOK && string(bytes.TrimSpace(body)) == challenge
}

// Add a subscription
func (h *Hub) addSubscription(callback, topic, secret string) {
	// Lock the mutex for writing since we'll modify the subscriptions map
	h.mutex.Lock()
	defer h.mutex.Unlock()

	// Create new subscription
	// Represents a single subscriber's intent to receive updates for a topic
	sub := Subscription{
		Callback: callback,
		Topic:    topic,
		Secret:   secret,
	}

	// Check if we already have any subscriptions for this topic
	//
	// Check if the value exists in a map: value, ok := someMap[key]
	subs, exists := h.subscriptions[topic]
	// If this is the first subscription for this topic
	if !exists {
		// Create a new slice containing just this subscription
		// and add it to the subscriptions map under this topic
		h.subscriptions[topic] = []Subscription{sub}
		return
	}

	// If we already have subscriptions for this topic,
	// check if this exact subscriber (callback URL) is already subscribed
	for i, existingSub := range subs {
		// If we find a matching callback, this is a subscription update
		if existingSub.Callback == callback {
			// Replace the existing subscription with the new one
			// 5.1 Subscriber Sends Subscription Request
			//
			// Hubs MUST allow subscribers to re-request subscriptions that are
			// already activated. Each subsequent request to a hub to subscribe
			// or unsubscribe MUST override the previous subscription state for
			// a specific topic URL and callback URL combination,
			subs[i] = sub
			h.subscriptions[topic] = subs
			return
		}
	}

	// If this is a new subscriber for an existing topic
	// (we didn't find a matching callback above)
	// Add this subscription to the end of the slice for this topic
	h.subscriptions[topic] = append(subs, sub)
}

// Remove an existing subscription from the hub.
func (h *Hub) removeSubscription(callback, topic string) {
	// Lock the mutex for writing since we'll modify the subscriptions map
	h.mutex.Lock()
	defer h.mutex.Unlock()

	// Check if the topic exists in our subscriptions map
	subs, exists := h.subscriptions[topic]

	// If the topic doesn't exist, there's nothing to remove, so exit early
	if !exists {
		return
	}

	// Create a new slice to hold all subscriptions except the one we're removing
	// Common pattern to filter slice elements in Go
	var updatedSubs []Subscription

	// Iterate through all existing subscriptions for this topic
	for _, sub := range subs {
		// Check if the sub callback matches the one we want to remove
		if sub.Callback != callback {
			// If it doesn't match, keep it by adding it to our filtered slice
			updatedSubs = append(updatedSubs, sub)
		}
		// Note: subscriptions with matching callbacks are simply not added
		// to the new slice (and thus removed)
	}

	// Update the topic's subscription list with our filtered version
	// This replaces the original slice with one that doesn't contain the removed subscription
	// Note: even if the list is empty, we keep the topic in the subscriptions map
	h.subscriptions[topic] = updatedSubs

	// Log that we've removed the subscription
	log.Printf("Removed subscription for callback '%s' from topic '%s'. Remaining subscriptions: %d",
		callback, topic, len(updatedSubs))
}

func main() {

	// Create a new WebSub hub
	hub := NewHub()

	// Now the hub is used
	hub.PrintStats()

	// Set up the web server to use our Subscribe function
	http.HandleFunc("/", hub.Subscribe)

	// Start the server on port 8080
	log.Println("WebSub Hub starting on port: 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
