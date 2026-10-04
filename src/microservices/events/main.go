package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

var topics = []string{"movie-events", "user-events", "payment-events"}

var producer sarama.SyncProducer

type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Payload   any       `json:"payload"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type,omitempty"`
}

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	var err error
	for {
		producer, err = sarama.NewSyncProducer(brokers, config)
		if err == nil {
			break
		}
		log.Printf("Waiting for kafka: %v", err)
		time.Sleep(3 * time.Second)
	}
	defer producer.Close()

	group, err := sarama.NewConsumerGroup(brokers, "events-service", config)
	if err != nil {
		log.Fatal(err)
	}
	defer group.Close()
	go consume(group)

	http.HandleFunc("GET /api/events/health", handleHealth)
	http.HandleFunc("POST /api/events/movie", handleMovieEvent)
	http.HandleFunc("POST /api/events/user", handleUserEvent)
	http.HandleFunc("POST /api/events/payment", handlePaymentEvent)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	var e MovieEvent
	if !decode(w, r, &e) {
		return
	}
	if e.MovieID == 0 || e.Title == "" || e.Action == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "movie_id, title and action are required"})
		return
	}
	publish(w, "movie", fmt.Sprintf("movie-%d-%s", e.MovieID, e.Action), e)
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	var e UserEvent
	if !decode(w, r, &e) {
		return
	}
	if e.UserID == 0 || e.Action == "" || e.Timestamp.IsZero() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user_id, action and timestamp are required"})
		return
	}
	publish(w, "user", fmt.Sprintf("user-%d-%s", e.UserID, e.Action), e)
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	var e PaymentEvent
	if !decode(w, r, &e) {
		return
	}
	if e.PaymentID == 0 || e.UserID == 0 || e.Amount == 0 || e.Status == "" || e.Timestamp.IsZero() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payment_id, user_id, amount, status and timestamp are required"})
		return
	}
	publish(w, "payment", fmt.Sprintf("payment-%d-%s", e.PaymentID, e.Status), e)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return false
	}
	return true
}

func publish(w http.ResponseWriter, eventType, id string, payload any) {
	event := Event{ID: id, Type: eventType, Timestamp: time.Now().UTC(), Payload: payload}
	value, err := json.Marshal(event)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: eventType + "-events",
		Key:   sarama.StringEncoder(id),
		Value: sarama.ByteEncoder(value),
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func consume(group sarama.ConsumerGroup) {
	for {
		if err := group.Consume(context.Background(), topics, consumer{}); err != nil {
			log.Printf("Consumer error: %v", err)
			time.Sleep(3 * time.Second)
		}
	}
}

type consumer struct{}

func (consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			log.Printf("Processed event from %s partition=%d offset=%d: %s", msg.Topic, msg.Partition, msg.Offset, msg.Value)
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
