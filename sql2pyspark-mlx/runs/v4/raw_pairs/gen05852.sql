SELECT id, is_flagged, txn_type, merchant, MONTH(txn_time) AS txn_time_month FROM transactions
