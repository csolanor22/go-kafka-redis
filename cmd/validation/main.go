package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"go-kafka-redis/internal/common"

	"github.com/segmentio/kafka-go"
)

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	// Producer for Results
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        common.TopicValidationResult,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	// Consumer for Commands
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		GroupID:        "validation-service-group",
		Topic:          common.TopicValidationCommand,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 500 * time.Millisecond,
	})
	defer reader.Close()

	log.Println("Validation service started (Kafka)...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Consumer error: %v", err)
			break
		}

		var tx common.Transaction
		if err := json.Unmarshal(m.Value, &tx); err != nil {
			log.Printf("Failed to unmarshal: %v", err)
			continue
		}

		log.Printf("[Validation] Checking transaction %s", tx.ID)

		valid := true
		if tx.Amount <= 0 {
			valid = false
		}
		if tx.Currency == "" {
			valid = false
		}

		if valid {
			tx.Status = common.StatusValidated
		} else {
			tx.Status = common.StatusFailed
		}

		// Send Result
		data, _ := json.Marshal(tx)
		if err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(tx.ID),
				Value: data,
			},
		); err != nil {
			log.Printf("Failed to write result: %v", err)
		} else {
			log.Printf("[Validation] Result sent for %s: %s", tx.ID, tx.Status)
		}
	}
}
