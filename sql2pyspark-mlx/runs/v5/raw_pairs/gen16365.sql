SELECT amount, txn_time, is_flagged
FROM transactions
WHERE NOT (account_id IN (2, 7, 14, 28) AND merchant IN ('Amazon', 'Starbucks'))
