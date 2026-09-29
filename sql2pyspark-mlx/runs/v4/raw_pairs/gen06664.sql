SELECT id, amount FROM transactions WHERE amount <= (SELECT MIN(amount) FROM transactions WHERE txn_time >= '2025-03-24') ORDER BY 1 LIMIT 5
