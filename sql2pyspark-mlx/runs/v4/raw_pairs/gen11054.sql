SELECT * FROM (SELECT id, txn_type, amount, is_flagged, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) rn FROM transactions) ranked WHERE rn = 1 ORDER BY amount
