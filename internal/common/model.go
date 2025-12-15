package common

import "time"

// Transaction represents the payment transaction moving through the system.
type Transaction struct {
	ID             string    `json:"id"`
	Amount         float64   `json:"amount"`
	Currency       string    `json:"currency"`
	Merchant       string    `json:"merchant_id"`
	AccountID      string    `json:"account_id"`
	Timestamp      time.Time `json:"timestamp"`
	Status         string    `json:"status"`          // PENDING, VALIDATED, CHECKED, SAVED, AUTHORIZED, FAILED, REJECTED
	Step           string    `json:"step"`            // Current step in the saga
	ValidationDone bool      `json:"validation_done"` // Saga parallel tracking
	EnrichmentDone bool      `json:"enrichment_done"` // Saga parallel tracking
	EnrichmentData string    `json:"enrichment_data"` // Result from enrichment
}

const (
	StatusPending    = "PENDING"
	StatusValidated  = "VALIDATED"
	StatusChecked    = "CHECKED"
	StatusSaved      = "SAVED"
	StatusAuthorized = "AUTHORIZED"
	StatusFailed     = "FAILED"
	StatusRejected   = "REJECTED"
)

const (
	TopicTransactions = "transactions"

	// Worker topics
	TopicValidationCommand = "validation.command"
	TopicValidationResult  = "validation.result"
	TopicEnrichmentCommand = "enrichment.command"
	TopicEnrichmentResult  = "enrichment.result"
	TopicFraudCommand      = "fraud.command"
	TopicFraudResult       = "fraud.result"
	TopicLedgerCommand     = "ledger.command"
	TopicLedgerResult      = "ledger.result"
	TopicAuthCommand       = "auth.command"
	TopicAuthResult        = "auth.result"

	TopicTransactionCompleted = "transaction.completed"
)
