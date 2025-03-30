package main

import (
	"fmt"
	"net/url"
)

// Print information about the hub
// Useful when debugging to avoid "declared and not used" errors
func (hub *Hub) PrintStats() {
	fmt.Printf("WebSub Hub with %d topics\n", len(hub.subscriptions))
}

// Print form values for debugging
func DebugFormValues(prefix string, form url.Values) {
	fmt.Println(prefix)
	fmt.Println("-------------------------------------")
	fmt.Println("All form values:\n")
	for key, values := range form {
		fmt.Printf("%s: %v\n", key, values)
	}
	fmt.Println("-------------------------------------")
}
