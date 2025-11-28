-- Make accounts.session_path long enough for StringSession text
ALTER TABLE accounts
  MODIFY COLUMN session_path TEXT;
