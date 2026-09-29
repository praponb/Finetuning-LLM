SELECT id, amount, txn_type, COALESCE(merchant, 'N/A') AS merchant_filled FROM transactions WHERE account_id IN (13, 23) ORDER BY merchant_filled;
