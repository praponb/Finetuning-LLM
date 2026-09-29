SELECT CASE WHEN txn_type IN ('DEBIT', 'FEE') THEN 'group_a' ELSE 'other' END AS txn_type_group, QUARTER(txn_time) AS txn_quarter, COUNT(amount) AS amount_count
FROM transactions
WHERE amount IS NOT NULL
GROUP BY txn_type_group, txn_quarter
