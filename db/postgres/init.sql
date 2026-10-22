-- Operational database for the agent layer (Phase 2).
-- Analytics data (facts/dims) lives in DuckDB via dbt, NOT here.

CREATE SCHEMA IF NOT EXISTS aml;

-- One row per alert raised by the detection layer
CREATE TABLE IF NOT EXISTS aml.alerts (
    alert_id        TEXT PRIMARY KEY,
    account_id      TEXT        NOT NULL,
    customer_id     TEXT        NOT NULL,
    rule_codes      TEXT[]      NOT NULL,          -- e.g. {STRUCTURING, VELOCITY}
    detection_score NUMERIC(5,2),
    transaction_ids TEXT[]      NOT NULL,          -- triggering transactions
    status          TEXT        NOT NULL DEFAULT 'NEW'
                    CHECK (status IN ('NEW','INVESTIGATING','PENDING_REVIEW','CLOSED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One investigation case per alert, produced by the agents
CREATE TABLE IF NOT EXISTS aml.cases (
    case_id         TEXT PRIMARY KEY,
    alert_id        TEXT        NOT NULL REFERENCES aml.alerts(alert_id),
    risk_score      NUMERIC(5,2),                  -- computed by deterministic code
    risk_factors    JSONB       NOT NULL DEFAULT '[]',
    sar_narrative   TEXT,                          -- drafted by the Narrative agent
    citations       JSONB       NOT NULL DEFAULT '[]', -- [{transaction_id, field, value}]
    citations_valid BOOLEAN,
    revision        INT         NOT NULL DEFAULT 1,
    status          TEXT        NOT NULL DEFAULT 'DRAFT'
                    CHECK (status IN ('DRAFT','PENDING_REVIEW','CHANGES_REQUESTED','APPROVED','REJECTED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Human-in-the-loop decisions (the sign-off trail)
CREATE TABLE IF NOT EXISTS aml.review_decisions (
    decision_id     BIGSERIAL PRIMARY KEY,
    case_id         TEXT        NOT NULL REFERENCES aml.cases(case_id),
    reviewer        TEXT        NOT NULL,
    decision        TEXT        NOT NULL CHECK (decision IN ('APPROVE','REJECT','REQUEST_CHANGES')),
    comment         TEXT,
    case_revision   INT         NOT NULL,
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only audit log: every agent step, SQL query, and tool call
CREATE TABLE IF NOT EXISTS aml.audit_log (
    event_id        BIGSERIAL PRIMARY KEY,
    case_id         TEXT,
    actor           TEXT        NOT NULL,   -- 'sql_agent', 'risk_agent', 'reviewer:jane', ...
    action          TEXT        NOT NULL,   -- 'SQL_QUERY', 'TOOL_CALL', 'LLM_CALL', 'DECISION'
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Make the audit log append-only: block UPDATE and DELETE
CREATE OR REPLACE FUNCTION aml.forbid_audit_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_log_no_mutation ON aml.audit_log;
CREATE TRIGGER audit_log_no_mutation
    BEFORE UPDATE OR DELETE ON aml.audit_log
    FOR EACH ROW EXECUTE FUNCTION aml.forbid_audit_mutation();

CREATE INDEX IF NOT EXISTS idx_alerts_status   ON aml.alerts(status);
CREATE INDEX IF NOT EXISTS idx_cases_status    ON aml.cases(status);
CREATE INDEX IF NOT EXISTS idx_audit_case      ON aml.audit_log(case_id, created_at);
