SELECT id, is_flagged, merchant, txn_time FROM transactions WHERE txn_type = 'REFUND' AND txn_type != 'DEBIT' AND txn_type IN ('FEE', 'DEBIT', 'REFUND') ORDER BY txn_time
