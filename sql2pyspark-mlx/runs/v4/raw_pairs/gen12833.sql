SELECT txn_type, is_flagged, amount
FROM transactions
WHERE amount < 3400 AND (txn_time < '2023-03-01' OR is_flagged);
