SELECT status FROM accounts WHERE status NOT IN ('FROZEN', 'CLOSED') INTERSECT SELECT status FROM payments ORDER BY status
