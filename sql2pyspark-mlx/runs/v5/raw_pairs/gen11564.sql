SELECT txn_type, SUM(amount) total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM transactions GROUP BY txn_type;
