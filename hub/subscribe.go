package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
)

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

	// Verify HTTP method is POST
	if request.Method != "POST" {
		errorMsg := "Method not allowed. WebSub requires HTTP POST."
		log.Println(errorMsg)
		http.Error(writer, errorMsg, http.StatusMethodNotAllowed)
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
		log.Printf("Received subscribe request. Topic: '%s', Callback: '%s'", topic, callback)
	} else if mode == "unsubscribe" {
		log.Printf("Received unsubscribe request. Topic: '%s', Callback: '%s'", topic, callback)
	}

	// Verification
	// Verification happens asynchronously after sending 202 response
	// 5.1.2 Subscription Response Details
	// The hub SHOULD perform the verification and validation of intent as soon as possible.
	go func() {

		// 8.2 Subscriptions
		// When performing intent verification, the hub SHOULD use a random, single-use hub.challenge.
		// Generate a random challenge string
		challenge := generateChallenge()

		// Verify intent by sending a GET request to the callback URL
		// Confirms the subscription request came from someone who controls the callback URL
		// Prevents attackers from subscribing someone else's URL to a topic
		intentVerified := hub.verifyIntent(callback, mode, topic, challenge)

		if !intentVerified {
			log.Printf("Failed to verify intent for %s request. Topic: %s, Callback: %s",
				mode, topic, callback)
			return
		}

		// Process based on verified intent
		if mode == "subscribe" {
			hub.addSubscription(callback, topic, secret)
			log.Printf("Verified and subscribed. Topic: %s, Callback: %s", topic, callback)
		} else {
			hub.removeSubscription(callback, topic)
			log.Printf("Verified and unsubscribed. Topic: %s, Callback: %s", topic, callback)
		}
	}()

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

// Send a verification request to the subscriber and verify the response
func (hub *Hub) verifyIntent(callback, mode, topic, challenge string) bool {

	// Build verification URL with query parameters
	callbackURL, err := url.Parse(callback)
	if err != nil {
		log.Printf("Invalid callback URL: %s", err)
		return false
	}

	// debugging values
	// log.Printf("Verifying intent: URL=%s, mode=%s, topic=%s, challenge=%s",
	//	callbackURL.String(), mode, topic, challenge)

	// Construct the callback query with parameters
	q := callbackURL.Query()
	q.Set("hub.mode", mode)
	q.Set("hub.topic", topic)
	q.Set("hub.challenge", challenge)
	callbackURL.RawQuery = q.Encode()

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

// Add a subscription to a topic
func (hub *Hub) addSubscription(callback, topic, secret string) {
	// Lock the mutex for writing since we'll modify the subscriptions map
	hub.mutex.Lock()

	// Check if topic exists in our subscriptions map
	if !topicExists(topic) {
		hub.mutex.Unlock() // Release lock before making external HTTP call
		hub.denySubscription(callback, topic, "Topic does not exist")
		return
	}
	// Cleanup
	defer hub.mutex.Unlock()

	// Create new subscription
	sub := Subscription{
		Callback: callback,
		Topic:    topic,
		Secret:   secret,
	}

	// Get or initialize the slice of subscriptions for this topic
	subs := hub.subscriptions[topic]

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
			hub.subscriptions[topic] = subs
			return
		}
	}

	// If this is a new subscriber for an existing topic
	// (we didn't find a matching callback above)
	// Add this subscription to the end of the slice for this topic
	hub.subscriptions[topic] = append(subs, sub)
}

// Remove an existing subscription from a topic
func (hub *Hub) removeSubscription(callback, topic string) {
	// Lock the mutex for writing since we'll modify the subscriptions map
	hub.mutex.Lock()
	defer hub.mutex.Unlock()

	// Check if the topic exists in our subscriptions map
	// If the topic doesn't exist, there's nothing to remove, so exit early
	subs, exists := hub.subscriptions[topic]
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
	hub.subscriptions[topic] = updatedSubs

	// Log that we've removed the subscription from the topic
	log.Printf("Removed subscription for callback '%s' from topic '%s'. Remaining subscriptions: %d",
		callback, topic, len(updatedSubs))
}
