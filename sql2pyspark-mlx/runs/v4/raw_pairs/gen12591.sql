SELECT id, txn_time, is_flagged, DATE_FORMAT(txn_time, 'yyyy-MM-dd') AS txn_time_label FROM transactions WHERE is_flagged = FALSE
