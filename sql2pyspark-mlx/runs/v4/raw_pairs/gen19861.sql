SELECT id, is_flagged, account_id, SUBSTRING(merchant, 1, 4) AS merchant_prefix FROM transactions WHERE txn_type <> 'REFUND'
