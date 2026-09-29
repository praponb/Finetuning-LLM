SELECT *, CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END AS is_7_eleven, LOWER(txn_type) AS txn_type_lower FROM transactions WHERE amount <= 2400 OR NOT is_flagged ORDER BY amount, id LIMIT 10
