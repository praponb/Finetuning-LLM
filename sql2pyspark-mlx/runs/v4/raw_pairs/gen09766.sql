SELECT id, txn_type, amount, merchant FROM (SELECT id, txn_type, amount, merchant, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS rn FROM transactions) AS r WHERE rn <= 5
