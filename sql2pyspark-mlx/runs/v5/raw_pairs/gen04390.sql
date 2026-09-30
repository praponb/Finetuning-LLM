SELECT DISTINCT merchant, txn_type
FROM transactions
WHERE txn_type != 'REFUND'
ORDER BY merchant, txn_type
LIMIT 5;
