SELECT id, is_flagged, CAST(is_flagged AS INT) AS is_flagged_flag FROM transactions WHERE account_id IN (23, 31);
