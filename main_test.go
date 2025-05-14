package main

import (
	"bytes"
	"encoding/json"
	"io"

	// "net/http"
	"net/http/httptest"
	// "reflect"
	"strconv"
	"testing"
	"time"
)

func TestConvertHandler_LargePayload(t *testing.T) {
	initWorkerPool()

	t.Log("Creating test payload...")
	payload := make(map[string]interface{})
	expected := make(map[string]interface{})
	for i := 0; i < 100; i++ {
		originalKey := "FieldName" + strconv.Itoa(i)
		snakeKey := "field_name_" + strconv.Itoa(i)
		payload[originalKey] = i
		expected[snakeKey] = float64(i) // JSON unmarshal uses float64
	}
	t.Logf("Created payload with %d items", len(payload))

	body, _ := json.Marshal(payload)
	req := httptest.NewRequest("POST", "/convert?mode=toSnake", bytes.NewReader(body))
	w := httptest.NewRecorder()

	t.Log("Starting conversion...")
	start := time.Now()
	convertHandler(w, req)
	duration := time.Since(start)
	t.Logf("Conversion completed in %v", duration)

	resp := w.Result()
	respBody, _ := io.ReadAll(resp.Body)
	t.Logf("Response status: %d", resp.StatusCode)

	var actual map[string]interface{}
	if err := json.Unmarshal(respBody, &actual); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	// Debug output
	t.Log("=== MAP COMPARISON ===")
	t.Logf("Expected keys: %d, Actual keys: %d", len(expected), len(actual))

	mismatchCount := 0
	for k, expectedVal := range expected {
		actualVal, exists := actual[k]
		if !exists {
			t.Logf("MISSING KEY: %s", k)
			mismatchCount++
			continue
		}
		if expectedVal != actualVal {
			t.Logf("VALUE MISMATCH: %s (expected %v, got %v)", k, expectedVal, actualVal)
			mismatchCount++
		}
	}

	if mismatchCount > 0 {
		t.Logf("Total mismatches: %d", mismatchCount)
	} else {
		t.Log("All key-value pairs match perfectly!")
	}

	if !t.Failed() {
		t.Log("TEST PASSED SUCCESSFULLY")
	}
}
