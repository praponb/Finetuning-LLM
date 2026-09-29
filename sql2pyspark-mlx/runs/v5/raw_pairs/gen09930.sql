SELECT txn_time, merchant, is_flagged, CAST(amount AS INT) amount_int FROM transactions
