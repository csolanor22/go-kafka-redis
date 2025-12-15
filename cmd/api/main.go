package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"go-kafka-redis/internal/common"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

var (
	writer *kafka.Writer
	rm     *ResponseManager
)

// ResponseManager manages pending requests
type ResponseManager struct {
	mu      sync.Mutex
	pending map[string]chan common.Transaction
}

func NewResponseManager() *ResponseManager {
	return &ResponseManager{
		pending: make(map[string]chan common.Transaction),
	}
}

func (rm *ResponseManager) Register(id string) chan common.Transaction {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	ch := make(chan common.Transaction, 1)
	rm.pending[id] = ch
	return ch
}

func (rm *ResponseManager) Unregister(id string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if ch, ok := rm.pending[id]; ok {
		close(ch)
		delete(rm.pending, id)
	}
}

func (rm *ResponseManager) Dispatch(tx common.Transaction) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if ch, ok := rm.pending[tx.ID]; ok {
		ch <- tx
	}
}

func main() {
	// Initialize Kafka Writer
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	logger := log.New(os.Stdout, "kafka-writer: ", 0)
	writer = &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        common.TopicTransactions,
		Balancer:     &kafka.LeastBytes{},
		Logger:       kafka.LoggerFunc(logger.Printf),
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	// Initialize Response Manager
	rm = NewResponseManager()

	// Start Completion Consumer
	go startCompletionConsumer(kafkaBroker)

	r := gin.Default()
	r.POST("/authorize", authorizeHandler)

	log.Println("Producer starting on :8080")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func startCompletionConsumer(broker string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          common.TopicTransactionCompleted,
		GroupID:        "api-gateway-response-group", // Unique per instance in real world
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 500 * time.Millisecond,
	})
	defer reader.Close()

	log.Println("API listening for completion events...")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Consumer error: %v", err)
			continue // Retry
		}

		var tx common.Transaction
		if err := json.Unmarshal(m.Value, &tx); err != nil {
			log.Printf("Failed to unmarshal completion: %v", err)
			continue
		}

		rm.Dispatch(tx)
	}
}

func authorizeHandler(c *gin.Context) {
	var req common.Transaction
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Basic ID generation if missing
	if req.ID == "" {
		req.ID = time.Now().Format("20060102150405")
	}
	req.Status = common.StatusPending
	req.Timestamp = time.Now()

	// Register for response
	respChan := rm.Register(req.ID)
	defer rm.Unregister(req.ID)

	// Serialize to JSON
	msgBytes, err := json.Marshal(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize transaction"})
		return
	}

	// Publish to Kafka
	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(req.ID),
			Value: msgBytes,
		},
	)
	if err != nil {
		log.Printf("Failed to write message: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish transaction"})
		return
	}

	// Wait for response with timeout
	select {
	case finalTx := <-respChan:
		c.JSON(http.StatusOK, finalTx)
	case <-time.After(30 * time.Second):
		c.JSON(http.StatusGatewayTimeout, gin.H{
			"error":   "Timeout waiting for transaction completion",
			"id":      req.ID,
			"status":  "PENDING", // Still pending/processing
			"message": "Transaction submitted but not completed in time",
		})
	}
}
