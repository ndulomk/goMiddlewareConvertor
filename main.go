package main

import (
	"encoding/json"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/iancoleman/strcase"
)

// Worker pool size based on available CPUs
var numWorkers = runtime.NumCPU()

// Job represents a key conversion job
type Job struct {
	Key    string
	Value  interface{}
	Mode   string
	Result chan KeyValuePair
}

// KeyValuePair represents a converted key and its value
type KeyValuePair struct {
	Key   string
	Value interface{}
}

// Worker pool
var jobQueue chan Job
var once sync.Once

// Cache for commonly used conversions
var cacheSnake sync.Map
var cacheCamel sync.Map

func initWorkerPool() {
	once.Do(func() {
		jobQueue = make(chan Job, 1000) // Buffer for jobs

		// Start workers
		for i := 0; i < numWorkers; i++ {
			go worker()
		}
		log.Printf("Started %d workers in the conversion pool", numWorkers)
	})
}

// Worker processes jobs from the queue
func worker() {
	for job := range jobQueue {
		var newKey string
		var cache *sync.Map

		if job.Mode == "toSnake" {
			cache = &cacheSnake
			// Check if it's already cached
			if cachedKey, ok := cache.Load(job.Key); ok {
				newKey = cachedKey.(string)
			} else {
				newKey = strcase.ToSnake(job.Key)
				cache.Store(job.Key, newKey)
			}
		} else {
			cache = &cacheCamel
			// Check if it's already cached
			if cachedKey, ok := cache.Load(job.Key); ok {
				newKey = cachedKey.(string)
			} else {
				newKey = strcase.ToLowerCamel(job.Key)
				cache.Store(job.Key, newKey)
			}
		}

		job.Result <- KeyValuePair{Key: newKey, Value: job.Value}
	}
}

func convertHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	// Set response headers early for better performance
	w.Header().Set("Content-Type", "application/json")

	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode != "toSnake" && mode != "toCamel" {
		mode = "toCamel" // Default to camelCase if not specified
	}

	var converted interface{}
	// Check if it's an array at the top level
	if arrayPayload, isArray := r.Context().Value("isArray").(bool); isArray && arrayPayload {
		converted = convertArrayConcurrent(payload["data"].([]interface{}), mode)
	} else {
		converted = convertKeysConcurrent(payload, mode)
	}

	json.NewEncoder(w).Encode(converted)

	log.Printf("Converted request in %v with mode %s", time.Since(startTime), mode)
}

// Handle array of objects
func arrayHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var payload []interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON Array", http.StatusBadRequest)
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode != "toSnake" && mode != "toCamel" {
		mode = "toCamel" // Default to camelCase if not specified
	}

	converted := convertArrayConcurrent(payload, mode)
	json.NewEncoder(w).Encode(converted)
}

// Convert array of objects concurrently
func convertArrayConcurrent(input []interface{}, mode string) []interface{} {
	if len(input) == 0 {
		return input
	}

	result := make([]interface{}, len(input))
	var wg sync.WaitGroup

	// Process each object in the array concurrently but with a limit
	// to avoid creating too many goroutines
	semaphore := make(chan struct{}, 20) // Limit concurrent goroutines

	for i, item := range input {
		if mapItem, ok := item.(map[string]interface{}); ok {
			wg.Add(1)
			semaphore <- struct{}{} // Acquire semaphore

			go func(idx int, obj map[string]interface{}) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore

				result[idx] = convertKeysConcurrent(obj, mode)
			}(i, mapItem)
		} else {
			// Non-map items pass through unchanged
			result[i] = item
		}
	}

	wg.Wait()
	return result
}

func convertKeysConcurrent(input map[string]interface{}, mode string) map[string]interface{} {
	// If the input is nil, return empty map
	if input == nil {
		return make(map[string]interface{})
	}

	result := make(map[string]interface{})

	// If the input is small, don't use concurrency
	if len(input) < 5 {
		return convertKeys(input, mode)
	}

	// Process each key-value pair
	var wg sync.WaitGroup
	resultMutex := sync.Mutex{}

	for k, v := range input {
		var newKey string
		var cache *sync.Map

		// Get the new key from cache if available
		if mode == "toSnake" {
			cache = &cacheSnake
			if cachedKey, ok := cache.Load(k); ok {
				newKey = cachedKey.(string)
			} else {
				newKey = strcase.ToSnake(k)
				cache.Store(k, newKey)
			}
		} else {
			cache = &cacheCamel
			if cachedKey, ok := cache.Load(k); ok {
				newKey = cachedKey.(string)
			} else {
				newKey = strcase.ToLowerCamel(k)
				cache.Store(k, newKey)
			}
		}

		// Process nested structures
		switch value := v.(type) {
		case map[string]interface{}:
			wg.Add(1)
			go func(key string, nestedMap map[string]interface{}) {
				defer wg.Done()
				nestedResult := convertKeysConcurrent(nestedMap, mode)
				resultMutex.Lock()
				result[key] = nestedResult
				resultMutex.Unlock()
			}(newKey, value)

		case []interface{}:
			wg.Add(1)
			go func(key string, nestedArray []interface{}) {
				defer wg.Done()

				// Convert array elements
				var processedArray []interface{}

				// Process arrays of maps
				hasMap := false
				for _, item := range nestedArray {
					if _, isMap := item.(map[string]interface{}); isMap {
						hasMap = true
						break
					}
				}

				if hasMap {
					processedArray = convertArrayConcurrent(nestedArray, mode)
				} else {
					processedArray = nestedArray
				}

				resultMutex.Lock()
				result[key] = processedArray
				resultMutex.Unlock()
			}(newKey, value)

		default:
			// Simple values can be added directly
			resultMutex.Lock()
			result[newKey] = v
			resultMutex.Unlock()
		}
	}

	wg.Wait()
	return result
}

// Original sequential method as fallback for small inputs
func convertKeys(input map[string]interface{}, mode string) map[string]interface{} {
	result := make(map[string]interface{})

	for k, v := range input {
		var newKey string
		var cache *sync.Map

		// Get the new key from cache if available
		if mode == "toSnake" {
			cache = &cacheSnake
			if cachedKey, ok := cache.Load(k); ok {
				newKey = cachedKey.(string)
			} else {
				newKey = strcase.ToSnake(k)
				cache.Store(k, newKey)
			}
		} else {
			cache = &cacheCamel
			if cachedKey, ok := cache.Load(k); ok {
				newKey = cachedKey.(string)
			} else {
				newKey = strcase.ToLowerCamel(k)
				cache.Store(k, newKey)
			}
		}

		// Handle nested objects and arrays
		switch value := v.(type) {
		case map[string]interface{}:
			result[newKey] = convertKeys(value, mode)

		case []interface{}:
			processedSlice := make([]interface{}, len(value))
			for i, item := range value {
				if itemMap, ok := item.(map[string]interface{}); ok {
					processedSlice[i] = convertKeys(itemMap, mode)
				} else {
					processedSlice[i] = item
				}
			}
			result[newKey] = processedSlice

		default:
			result[newKey] = v
		}
	}

	return result
}

// Bulk convert handler for large datasets
func bulkConvertHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var payload []map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON Bulk Array", http.StatusBadRequest)
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode != "toSnake" && mode != "toCamel" {
		mode = "toCamel"
	}

	// Process each item in the bulk array concurrently
	result := make([]map[string]interface{}, len(payload))
	var wg sync.WaitGroup

	// Use semaphore to limit concurrent goroutines
	semaphore := make(chan struct{}, numWorkers*2)

	for i, item := range payload {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire

		go func(idx int, obj map[string]interface{}) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release

			result[idx] = convertKeysConcurrent(obj, mode)
		}(i, item)
	}

	wg.Wait()
	json.NewEncoder(w).Encode(result)
}

func main() {
	// Initialize worker pool
	initWorkerPool()

	// Prewarm cache with common DB field names
	commonDbFields := []string{
		"id", "created_at", "updated_at", "deleted_at", "user_id",
		"client_id", "medicine_id", "supplier_id", "stock_id", "sale_id",
		"credit_note_id", "group_id", "name", "email", "phone", "address",
		"price", "quantity", "description", "status", "total", "discount",
		"payment_method", "payment_status", "shipping_address", "tax",
	}

	for _, field := range commonDbFields {
		cacheSnake.Store(strcase.ToLowerCamel(field), field)
		cacheCamel.Store(field, strcase.ToLowerCamel(field))
	}

	// Enable CORS
	corsMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			next.ServeHTTP(w, r)
		})
	}

	// Create a new ServeMux
	mux := http.NewServeMux()
	mux.HandleFunc("/convert", convertHandler)
	mux.HandleFunc("/convert/array", arrayHandler)
	mux.HandleFunc("/convert/bulk", bulkConvertHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Apply middleware
	handler := corsMiddleware(mux)

	// Configure server with timeouts
	server := &http.Server{
		Addr:         ":8081",
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Println("Go case converter is running on port 8081 with", numWorkers, "workers")
	log.Fatal(server.ListenAndServe())
}
