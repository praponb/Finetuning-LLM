SELECT TO_DATE(txn_time) AS txn_day, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM transactions GROUP BY TO_DATE(txn_time);
