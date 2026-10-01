package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/crypto/bcrypt"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(read *http.Request) bool { return true },
}

var broadcast = make(chan []byte)

type each struct {
	Name string
	Conn *websocket.Conn
}
type players struct {
	ID         bson.ObjectID `bson:"_id,omitempty"`
	PlayerEach []each        `bson:"players"`
	Key        []byte        `bson:"key"`
	Room       string        `bson:"room"`
	Owner      string        `bson:"owner"`
	Addr       string        `bson:"addr"`
}

var client *mongo.Client
var collection *mongo.Collection

type Parsed struct {
	Cat    string `json:"cat"`
	Word   string `json:"word"`
	Letter string `json:"letter"`
}

type join struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Addr string `json:"addr"`
	Key  string `json:"key"`
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
			"model": "openai/gpt-oss-20b",
			"messages": []map[string]string{
				{
					"role":    "system",
					"content": "Respond with exactly one word: true or false. No explanation. Be very strict with answers, confirm only dictionary proven words.",
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

func sendThroughChan(write http.ResponseWriter, read *http.Request) {
	conn, err := upgrader.Upgrade(write, read, nil)
	head := read.Header.Get("Pass")
	if head == "" {
		log.Println("Invalid room")
	}

	if err != nil {
		panic(err)
	}
	defer conn.Close()
	type Join struct {
		Name    string
		Passkey string
	}

	go handleMessages(head)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	go func() {
		defer func() {
			conn.Close()
		}()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Println("Read error:", err)
				break
			}

			var strs join
			err = json.Unmarshal(msg, &strs)
			if err != nil {
				broadcast <- msg
			}

			if strs.Type == "join" {
				val := handleJoin(strs, read, conn)
				if val[1] == "err" {
					conn.WriteMessage(websocket.TextMessage, []byte(val[0]))
				} else {
					broadcast <- msg
				}
			}
		}

	}()

	times := time.NewTicker(30 * time.Second)

	for range times.C {
		err := conn.WriteMessage(websocket.PingMessage, nil)
		if err != nil {
			log.Println("Ping err:", err)
			break
		}
	}
}

func handleJoin(strs join, read *http.Request, conn *websocket.Conn) []string {
	var get players
	err := collection.FindOne(read.Context(), bson.M{"addr": strs.Addr}).Decode(&get)
	if err == mongo.ErrNoDocuments {
		return []string{"Room doesn't exiat.", "err"}
	}

	if err != nil {
		return []string{"An error occured. Please try again.", "err"}
	}

	err = bcrypt.CompareHashAndPassword([]byte(get.Key), []byte(strs.Key))
	if err != nil {
		return []string{"Key is incorrect.", "err"}
	}
	if len(get.PlayerEach) >= 6 {
		return []string{"Too many players in room already.", "err"}
	}

	get.PlayerEach = append(get.PlayerEach, each{Name: strs.Name, Conn: conn})
	_, err = collection.UpdateOne(read.Context(), bson.M{"addr": strs.Addr}, get)
	if err != nil {
		return []string{"Failed to add you to room. Please try again.", "err"}
	}
	return []string{"Send through channels", "message"}
}

func handleMessages(word string) {
	var get players
	err := collection.FindOne(context.TODO(), bson.M{"addr": word}).Decode(&get)
	if err != nil {
		log.Println("Messaging err: ", err)
	}
	msg := <-broadcast
	for i, ch := range get.PlayerEach {
		err := ch.Conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			if get.Owner == ch.Name {
				collection.DeleteOne(context.TODO(), bson.M{"addr": word})
			} else {
				get.PlayerEach = append(get.PlayerEach[0:i], get.PlayerEach[i:]...)
				collection.UpdateOne(context.TODO(), bson.M{"addr": word}, get)
			}
		}
	}
}

func createRoom(write http.ResponseWriter, read *http.Request) {
	write.Header().Set("Access-Control-Allow-Origin", "*")
	write.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	write.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")

	if read.Method == "OPTIONS" {
		write.WriteHeader(200)
		return
	}

	if read.Method != "POST" {
		write.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(write).Encode(map[string]string{"err": "Invalid Method! Use a POST request only."})
		return
	}

	type get struct {
		Name     string `json:"name"`
		RoomName string `json:"roomName"`
		Time     string `json:"time"`
	}

	var gete get
	err := json.NewDecoder(read.Body).Decode(&gete)
	if err != nil {
		write.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(write).Encode(map[string]string{"err": "Invalid JSON."})
		return
	}

	defer read.Body.Close()

	if gete.Name == "" || len(gete.Name) > 30 {
		write.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(write).Encode(map[string]string{"err": "Room name must be between 1-30 characters."})
		return
	}

	getting := gete.Name + gete.RoomName + gete.Time

	key := make([]byte, 6)
	for i := 0; i < 6; i++ {
		key = append(key, getting[rand.Intn(len(getting))])
	}

	addrs := make([]byte, 20)
	for i := 0; i < 20; i++ {
		addrs = append(addrs, getting[rand.Intn(len(getting))])
	}

	pass, _ := bcrypt.GenerateFromPassword(key, 12)
	save := players{
		Room:       gete.RoomName,
		PlayerEach: []each{},
		Key:        pass,
		Owner:      gete.Name,
		Addr:       string(addrs),
	}
	_, err = collection.InsertOne(read.Context(), save)

	if err != nil {
		write.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(write).Encode(map[string]string{"err": "An error occured. Please check your connection."})
		return
	}
	json.NewEncoder(write).Encode(map[string]string{"addr": string(addrs), "key": string(key)})
}

func loadRooms(write http.ResponseWriter, read *http.Request) {
	write.Header().Set("Access-Control-Allow-Origin", "*")
	write.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	write.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")

	if read.Method == "OPTIONS" {
		write.WriteHeader(200)
		return
	}

	if read.Method != "GET" {
		write.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(write).Encode(map[string]string{"err": "Invalid Method! Use a POST request only."})
		return
	}

	type room struct {
		Name  string
		Addrs string
	}
	var str []room
	cursor, err := collection.Find(read.Context(), bson.M{})

	if err != nil {
		write.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(write).Encode(map[string]string{"err": "An error occured. Please check your connection."})
		return
	}
	defer cursor.Close(read.Context())
	for cursor.Next(read.Context()) {
		var tee players
		cursor.Decode(&tee)
		str = append(str, room{
			Name:  tee.Room,
			Addrs: tee.Addr,
		})
	}
	json.NewEncoder(write).Encode(map[string][]room{"message": str})
}

func main() {
	client, err := mongo.Connect(options.Client().ApplyURI(os.Getenv("MONGO")))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	collection = client.Database("MYTTN").Collection("online-users")
	http.HandleFunc("/calc", calculateNounAloneScore)
	http.HandleFunc("/transmit", sendThroughChan)
	http.HandleFunc("/loadrooms", loadRooms)
	http.HandleFunc("/createRoom", createRoom)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Println("Server running on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
