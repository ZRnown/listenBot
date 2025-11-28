-- Create table to store per-account listen whitelist sources
CREATE TABLE IF NOT EXISTS listen_sources (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  account_id BIGINT NOT NULL,
  source TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_ls_account (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
