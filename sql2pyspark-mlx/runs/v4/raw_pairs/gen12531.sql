SELECT txn_type, COUNT(amount) AS amount_count FROM transactions WHERE account_id IN (19, 21, 33) AND merchant IN ('Shell', 'Amazon', 'Lazada') AND amount BETWEEN 2400 AND 3700 GROUP BY txn_type;
