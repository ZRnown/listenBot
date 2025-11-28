CREATE TABLE IF NOT EXISTS accounts (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  phone VARCHAR(32),
  nickname VARCHAR(128),
  username VARCHAR(128),
  session_path VARCHAR(255) NOT NULL,
  status ENUM('active','inactive','error') DEFAULT 'active',
  last_seen DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS keywords (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  account_id BIGINT NOT NULL,
  keyword VARCHAR(255) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_keywords_account(account_id)
);

CREATE TABLE IF NOT EXISTS settings (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  scope ENUM('global','account') NOT NULL,
  account_id BIGINT NULL,
  name VARCHAR(128) NOT NULL,
  value TEXT,
  UNIQUE KEY uniq_scope_key(scope, account_id, name)
);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  account_id BIGINT NOT NULL,
  source_chat_id BIGINT,
  source_chat_title VARCHAR(255),
  sender_id BIGINT,
  sender_name VARCHAR(255),
  sender_username VARCHAR(255),
  message_text MEDIUMTEXT,
  matched_keyword VARCHAR(255),
  delivered_status ENUM('success','error') DEFAULT 'success',
  delivered_error TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_alerts_account(account_id)
);
