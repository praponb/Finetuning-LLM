SELECT id, txn_type, amount, txn_time FROM (SELECT id, txn_type, amount, txn_time, RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS rnk FROM transactions) AS r WHERE rnk = 1;
