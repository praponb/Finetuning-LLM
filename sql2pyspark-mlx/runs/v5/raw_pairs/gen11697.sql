SELECT id, txn_type, amount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS pos FROM transactions) AS r WHERE pos <= 3;
