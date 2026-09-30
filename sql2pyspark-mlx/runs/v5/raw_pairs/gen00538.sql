SELECT txn_type, SUM(amount) AS total_amount FROM transactions GROUP BY txn_type ORDER BY txn_type;
