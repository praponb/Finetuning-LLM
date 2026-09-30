SELECT is_flagged, amount, LENGTH(txn_type) AS txn_type_length
FROM transactions
WHERE is_flagged = TRUE AND amount > 1700 AND txn_type = 'FEE'
