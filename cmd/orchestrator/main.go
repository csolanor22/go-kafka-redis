package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go-kafka-redis/internal/common"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

var (
	rdb         *redis.Client
	kafkaBroker string
	// Writers for commands
	validationWriter *kafka.Writer
	enrichmentWriter *kafka.Writer // NEW
	fraudWriter      *kafka.Writer
	ledgerWriter     *kafka.Writer
	authWriter       *kafka.Writer
	completionWriter *kafka.Writer
	stateMutex       sync.Mutex // Global lock for state updates
)

func main() {
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	redisAddr := os.Getenv("REDIS_ADDR")

	// Init Redis
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Init Kafka Writers
	validationWriter = getKafkaWriter(common.TopicValidationCommand)
	enrichmentWriter = getKafkaWriter(common.TopicEnrichmentCommand) // NEW
	fraudWriter = getKafkaWriter(common.TopicFraudCommand)
	ledgerWriter = getKafkaWriter(common.TopicLedgerCommand)
	authWriter = getKafkaWriter(common.TopicAuthCommand)
	completionWriter = getKafkaWriter(common.TopicTransactionCompleted)

	defer validationWriter.Close()
	defer enrichmentWriter.Close() // NEW
	defer fraudWriter.Close()
	defer ledgerWriter.Close()
	defer authWriter.Close()
	defer completionWriter.Close()

	log.Println("Orchestrator started, listening for messages...")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var wg sync.WaitGroup
	wg.Add(6) // +1 for enrichment

	// 1. Listen for new Transactions
	go startConsumer(common.TopicTransactions, "orchestrator-tx-group", handleNewTransaction)

	// 2. Listen for Validation Results
	go startConsumer(common.TopicValidationResult, "orchestrator-validation-group", handleValidationResult)

	// 3. Listen for Enrichment Results
	go startConsumer(common.TopicEnrichmentResult, "orchestrator-enrichment-group", handleEnrichmentResult) // NEW

	// 4. Listen for Fraud Results
	go startConsumer(common.TopicFraudResult, "orchestrator-fraud-group", handleFraudResult)

	// 5. Listen for Ledger Results
	go startConsumer(common.TopicLedgerResult, "orchestrator-ledger-group", handleLedgerResult)

	// 6. Listen for Auth Results
	go startConsumer(common.TopicAuthResult, "orchestrator-auth-group", handleAuthResult)

	wg.Wait()
}

func getKafkaWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
	}
}

func startConsumer(topic, groupID string, handler func([]byte)) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,    // Fetch immediately
		MaxBytes:       10e6, // 10MB
		CommitInterval: 500 * time.Millisecond,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Consumer for %s error: %v", topic, err)
			break
		}
		go handler(m.Value)
	}
}

// ------ Handlers ------

func handleNewTransaction(data []byte) {
	var tx common.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		log.Printf("Failed to unmarshal tx: %v", err)
		return
	}
	log.Printf("[Orchestrator] New Transaction: %s. Starting Saga (Parallel Validation & Enrichment).", tx.ID)
	updateState(&tx, common.StatusPending)

	// Step 1: Parallel Fan-out
	sendKafkaCommand(validationWriter, &tx)
	sendKafkaCommand(enrichmentWriter, &tx)
}

func handleValidationResult(data []byte) {
	var tx common.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		log.Printf("Failed to unmarshal validation result: %v", err)
		return
	}

	if tx.Status == common.StatusFailed || tx.Status == common.StatusRejected {
		log.Printf("[Orchestrator] Transaction %s failed at Validation.", tx.ID)
		updateState(&tx, tx.Status)
		return
	}

	log.Printf("[Orchestrator] Validation OK for %s.", tx.ID)

	// Update state in Redis to mark validation done
	// First, fetch fresh state to avoid race condition on EnrichmentDone?
	// For simplicity in this POC, we trust the incoming message + Redis set/get pattern or better:
	// We should update the specific field.
	// NOTE: In a real system, we need atomic updates or locking. Here we'll read status from Redis?
	// Simpler approach for POC: Update fields in tx and persist. Race condition on Status?
	// The incoming 'tx' has the state of Validation worker. We need to merge with Redis state?
	// Let's rely on Redis keys being modified sequentially or use Lua.
	// OR: Just set flag and check 'tx.EnrichmentDone'? No, tx coming from Validation worker has 'EnrichmentDone=false'.

	// Correct Approach for POC without full locking:
	// 1. Get current state from Redis
	stateMutex.Lock()
	defer stateMutex.Unlock()

	currentTx := getState(tx.ID)
	currentTx.ValidationDone = true
	currentTx.Status = common.StatusValidated // Keep updating status
	updateState(currentTx, common.StatusValidated)

	// Check if parallel step is done
	if currentTx.EnrichmentDone {
		log.Printf("[Orchestrator] Both Validation and Enrichment done for %s. Next: Fraud", tx.ID)
		sendKafkaCommand(fraudWriter, currentTx)
	} else {
		log.Printf("[Orchestrator] Validation done for %s. Waiting for Enrichment...", tx.ID)
	}
}

func handleEnrichmentResult(data []byte) {
	var tx common.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		log.Printf("Failed to unmarshal enrichment result: %v", err)
		return
	}

	// 1. Get current state from Redis
	stateMutex.Lock()
	defer stateMutex.Unlock()

	currentTx := getState(tx.ID)
	currentTx.EnrichmentDone = true
	currentTx.EnrichmentData = tx.EnrichmentData

	// Merge logic: ensure we don't overwrite if validation failed?
	// If validation failed, status might be FAILED.
	if currentTx.Status == common.StatusFailed || currentTx.Status == common.StatusRejected {
		log.Printf("[Orchestrator] Ignoring Enrichment for %s because it already failed.", tx.ID)
		return
	}

	updateState(currentTx, currentTx.Status) // Save enrichment data

	// Check if parallel step is done
	if currentTx.ValidationDone {
		log.Printf("[Orchestrator] Both Validation and Enrichment done for %s. Next: Fraud", tx.ID)
		sendKafkaCommand(fraudWriter, currentTx)
	} else {
		log.Printf("[Orchestrator] Enrichment done for %s. Waiting for Validation...", tx.ID)
	}
}

func handleFraudResult(data []byte) {
	var tx common.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		log.Printf("Failed to unmarshal fraud result: %v", err)
		return
	}

	if tx.Status == common.StatusFailed || tx.Status == common.StatusRejected {
		log.Printf("[Orchestrator] Transaction %s failed at Fraud.", tx.ID)
		updateState(&tx, tx.Status)
		return
	}

	log.Printf("[Orchestrator] Fraud check OK for %s. Next: Ledger", tx.ID)
	updateState(&tx, common.StatusChecked)

	// Step 3: Send to Ledger
	sendKafkaCommand(ledgerWriter, &tx)
}

func handleLedgerResult(data []byte) {
	var tx common.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		log.Printf("Failed to unmarshal ledger result: %v", err)
		return
	}

	if tx.Status == common.StatusFailed || tx.Status == common.StatusRejected {
		log.Printf("[Orchestrator] Transaction %s failed at Ledger.", tx.ID)
		updateState(&tx, tx.Status)
		return
	}

	log.Printf("[Orchestrator] Ledger save OK for %s. Next: Auth", tx.ID)
	updateState(&tx, common.StatusSaved)

	// Step 4: Send to Auth
	sendKafkaCommand(authWriter, &tx)
}

func handleAuthResult(data []byte) {
	var tx common.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		log.Printf("Failed to unmarshal auth result: %v", err)
		return
	}

	if tx.Status == common.StatusFailed || tx.Status == common.StatusRejected {
		log.Printf("[Orchestrator] Transaction %s failed at Auth.", tx.ID)
		updateState(&tx, tx.Status)
		return
	}

	log.Printf("[Orchestrator] Transaction %s COMPLETED SUCCESSFULLY.", tx.ID)
	updateState(&tx, common.StatusAuthorized)
}

// ------ Helpers ------

func sendKafkaCommand(writer *kafka.Writer, tx *common.Transaction) {
	// We forward the transaction object.
	// The worker receives it, does work, updates status, and publishes result.
	data, _ := json.Marshal(tx)
	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(tx.ID),
			Value: data,
		},
	)
	if err != nil {
		log.Printf("Failed to write to kafka: %v", err)
	}
}

func updateState(tx *common.Transaction, status string) {
	tx.Status = status
	tx.Step = status

	ctx := context.Background()
	key := fmt.Sprintf("transaction:%s", tx.ID)

	data, _ := json.Marshal(tx)
	rdb.Set(ctx, key, data, 0)
	log.Printf("State updated for %s: %s", tx.ID, status)

	// Publish to Completion Topic if terminal state
	if status == common.StatusAuthorized || status == common.StatusFailed || status == common.StatusRejected {
		sendKafkaCommand(completionWriter, tx)
		log.Printf("Published completion event for %s", tx.ID)
	}
}

func getState(id string) *common.Transaction {
	ctx := context.Background()
	key := fmt.Sprintf("transaction:%s", id)
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return &common.Transaction{ID: id} // Should not happen in middle of saga
	}
	var tx common.Transaction
	json.Unmarshal([]byte(val), &tx)
	return &tx
}
