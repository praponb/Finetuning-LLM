SELECT id, merchant, txn_type, CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END AS is_grab FROM transactions ORDER BY txn_type;
