SELECT txn_type, CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END AS is_refund
FROM transactions
WHERE amount >= 2300
ORDER BY is_refund;
