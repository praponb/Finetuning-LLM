SELECT id, is_flagged, txn_time FROM transactions WHERE NOT is_flagged AND amount >= 1000
