package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
)

func calculateNounAloneScore(write http.ResponseWriter, read *http.Request) {
	write.Header().Set("Access-Control-Allow-Origin", "*")
	write.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	write.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	read.Body = http.MaxBytesReader(write, read.Body, 1024*2)
	err := read.ParseForm()
	if err != nil {
		write.WriteHeader(http.StatusNotAcceptable)
		json.NewEncoder(write).Encode(map[string]string{"err": "the size of your words are too big."})
		return
	}
	type parsed struct {
		Cat    string `json:"cat"`
		Word   string `json:"word"`
		Letter string `json:"letter"`
	}
	type groqJson struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	var val []parsed
	var vex []string
	err = json.NewDecoder(read.Body).Decode(&val)
	if err != nil {
		write.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(write).Encode(map[string]string{"err": "Invalid JSON. please input proper JSON"})
		return
	}
	for _, ch := range val {
		body := map[string]interface{}{
			"model": "llama-3.1-8b-instant",
			"messages": []map[string]string{
				{"role": "system", "content": "You are only to answer correct questions with 'true' and wrong questions with 'false'."},
				{"role": "user", "content": "Is " + ch.Word + " a/an " + ch.Cat + " that begins with the letter " + ch.Letter + "?"},
			},
		}
		jsonData, _ := json.Marshal(body)
		resp, _ := http.NewRequest("POST", "https://api.groq.com/openai/v1/chat/completions", bytes.NewBuffer(jsonData))
		resp.Header.Set("Content-Type", "application/json")
		resp.Header.Set("Authorization", "Bearer "+os.Getenv("GROQ_KEY"))
		client := &http.Client{}
		get, err := client.Do(resp)
		if err != nil {
			write.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(write).Encode(map[string]string{"err": "Failed to load results. Please check your internet connection!"})
			return
		}
		defer get.Body.Close()
		vat, _ := io.ReadAll(get.Body)
		var groq groqJson
		json.Unmarshal(vat, &groq)
		ret := groq.Choices[0].Message.Content
		vex = append(vex, ret)
	}
	json.NewEncoder(write).Encode(map[string][]string{"message": vex})
}

func main() {
	http.HandleFunc("/calc", calculateNounAloneScore)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
