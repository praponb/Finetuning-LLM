SELECT id, status, customer_id, balance, LOWER(status) AS status_lower FROM accounts WHERE status IN ('FROZEN', 'CLOSED') AND account_type = 'LOAN';
