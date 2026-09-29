SELECT id, is_flagged
FROM transactions
WHERE account_id = 27 AND HOUR(txn_time) BETWEEN 9 AND 15
ORDER BY id ASC;
