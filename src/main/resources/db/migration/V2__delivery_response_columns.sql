-- Add columns to track the latest delivery attempt result
ALTER TABLE deliveries ADD COLUMN last_status_code INT;
ALTER TABLE deliveries ADD COLUMN last_error TEXT;