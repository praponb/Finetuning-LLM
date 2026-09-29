SELECT id, is_flagged, txn_time, amount, COALESCE(merchant, 'N/A') AS merchant_filled FROM transactions WHERE account_id = 27
