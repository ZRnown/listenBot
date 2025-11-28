ALTER TABLE keywords ADD COLUMN kind VARCHAR(16) NOT NULL DEFAULT 'listen' AFTER account_id;
CREATE INDEX idx_keywords_account_kind ON keywords(account_id, kind);
