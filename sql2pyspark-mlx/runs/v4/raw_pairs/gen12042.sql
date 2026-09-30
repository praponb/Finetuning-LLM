SELECT id, txn_type, CONCAT(txn_type, '/', merchant) AS txn_type_merchant FROM transactions WHERE amount IS NOT NULL ORDER BY id LIMIT 20;
