package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
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

// SubscribeHandler handles WebSub subscription requests (Subscriber -> Hub)
// Subscriber makes a POST to the hub to subscribe to updates about topic
//
// A function which takes a "receiver" (argument before func name) is called a method in Go
// The receiver is a special parameter that allows you to call the method on the struct
// The receiver is similar to the self parameter in Python
//
// ResponseWriter is an interface that allows you to write HTTP responses
// (modify in-place rather than return-based approach)
// Request is a struct that represents an HTTP request
func (hub *Hub) SubscribeHandler(writer http.ResponseWriter, request *http.Request) {

	// Fail fast philosophy. return exits the function immediately, preventing further execution. Common Go pattern

	// Only accept POST requests
	if request.Method != "POST" {
		http.Error(writer, "HTTP method is not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the form data from the request
	err := request.ParseForm()
	if err != nil {
		http.Error(writer, "Failed to parse request form data", http.StatusBadRequest)
		return
	}

	// Print form values for debugging
	debugFormValues("WebSub subscription request received:", request.Form)

	// https://www.w3.org/TR/websub/#hubs
	// A conforming hub:
	// 	* MUST accept a subscription request with the parameters hub.callback, hub.mode and hub.topic.
	// 	* MUST accept a subscription request with a hub.secret parameter.
	// 	* MAY respect the requested lease duration in subscription requests. (not included from websub-client)
	callback := request.Form.Get("hub.callback")
	mode := request.Form.Get("hub.mode")
	topic := request.Form.Get("hub.topic")
	secret := request.Form.Get("hub.secret")

	// Add validation rules for the parameters
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

	if mode == "subscribe" {
		log.Printf("Subscription request for topic '%s' with callback '%s'", topic, callback)
		if secret != "" {
			log.Printf("Secret provided for subscription")
		} else {
			log.Printf("No secret provided for subscription")
		}
	}

	// Tell the subscriber we got their request
	writer.WriteHeader(http.StatusAccepted)

}

// Print information about the hub
// Useful when debugging to avoid "declared and not used" errors
func (hub *Hub) PrintStats() {
	fmt.Printf("WebSub Hub with %d topics\n", len(hub.subscriptions))
}

// Print form values for debugging
func debugFormValues(prefix string, form url.Values) {
	fmt.Println(prefix)
	fmt.Println("-------------------------------------")
	fmt.Println("All form values:\n")
	for key, values := range form {
		fmt.Printf("%s: %v\n", key, values)
	}
	fmt.Println("-------------------------------------")
}

func main() {

	// Create a new WebSub hub
	hub := NewHub()

	// Now the hub is used
	// hub.PrintStats()

	// Set up the web server to use our SubscribeHandler function
	http.HandleFunc("/", hub.SubscribeHandler)

	// Start the server on port 8080
	log.Println("WebSub Hub starting on port: 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
