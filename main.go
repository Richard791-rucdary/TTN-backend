package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

type Parsed struct {
	Cat    string `json:"cat"`
	Word   string `json:"word"`
	Letter string `json:"letter"`
}

type GroqResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func calculateNounAloneScore(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, `{"err":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 2048)

	var input []Parsed
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"err": "Invalid JSON",
		})
		return
	}

	apiKey := os.Getenv("GROQ_KEY")
	if apiKey == "" {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"err": "GROQ_KEY environment variable is missing",
		})
		return
	}

	var results []string

	for _, item := range input {
		body := map[string]interface{}{
			"model": "llama-3.1-8b-instant",
			"messages": []map[string]string{
				{
					"role": "system",
					"content": "Respond with exactly one word: true or false. No explanation.",
				},
				{
					"role": "user",
					"content": fmt.Sprintf(
						"Is %s a %s that begins with the letter %s?",
						item.Word,
						item.Cat,
						item.Letter,
					),
				},
			},
		}

		jsonData, err := json.Marshal(body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"err": "Failed to create request",
			})
			return
		}

		req, err := http.NewRequest(
			"POST",
			"https://api.groq.com/openai/v1/chat/completions",
			bytes.NewBuffer(jsonData),
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"err": "Failed to create HTTP request",
			})
			return
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Println("Groq request failed:", err)

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"err": "Failed to contact Groq API",
			})
			return
		}

		responseBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"err": "Failed to read Groq response",
			})
			return
		}

		if resp.StatusCode != http.StatusOK {
			log.Println("Groq error:", string(responseBody))

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"err": string(responseBody),
			})
			return
		}

		var groq GroqResponse
		if err := json.Unmarshal(responseBody, &groq); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"err": "Failed to parse Groq response",
			})
			return
		}

		if len(groq.Choices) == 0 {
			log.Println("Empty Groq response:", string(responseBody))

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"err": "Groq returned no choices",
			})
			return
		}

		results = append(results, groq.Choices[0].Message.Content)
	}

	json.NewEncoder(w).Encode(map[string][]string{
		"message": results,
	})
}

func main() {
	http.HandleFunc("/calc", calculateNounAloneScore)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Println("Server running on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
