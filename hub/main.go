package main

import (
	"log"
	"net/http"
	"sync"
)

// Store topic-to-subscriptions mappings
// Struct is similar to a Python dataclass
type Subscription struct {
	Callback string
	Topic    string
	Secret   string
}

// Map is similar to a Python dict
type Hub struct {
	subscriptions map[string][]Subscription
	mutex         sync.RWMutex // Ensures thread-safe access to the subscriptions map by multiple goroutines
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

func main() {

	// Create a new WebSub hub
	hub := NewHub()

	// Register endpoints for subscription and publishing
	http.HandleFunc("/", hub.Subscribe)
	http.HandleFunc("/publish", hub.PublishContent)

	// Start the server on port 8080
	log.Println("Hub starting on port: 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
