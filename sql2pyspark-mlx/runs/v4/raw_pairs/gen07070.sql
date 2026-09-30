SELECT *, LENGTH(status) AS status_length, LOWER(account_type) AS account_type_lower
FROM accounts
WHERE status IN ('OPEN', 'FROZEN', 'CLOSED') AND status <> 'OPEN';
