-- Initialize database for Webhook Ingestion Service

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(128) PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0,
    currency VARCHAR(8) NOT NULL DEFAULT 'NGN',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL,
    scope VARCHAR(64) NOT NULL DEFAULT 'ledger',
    status VARCHAR(32) NOT NULL DEFAULT 'COMMITTED',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) UNIQUE NOT NULL,
    payment_id VARCHAR(128) NOT NULL,
    account_id VARCHAR(128) NOT NULL REFERENCES accounts(account_id),
    event_type VARCHAR(64) NOT NULL,
    sequence INTEGER NOT NULL DEFAULT 1,
    amount BIGINT NOT NULL,
    currency VARCHAR(8) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account ON ledger_entries(account_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_payment ON ledger_entries(payment_id);
