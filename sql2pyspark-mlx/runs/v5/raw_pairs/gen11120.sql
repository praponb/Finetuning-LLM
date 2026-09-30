SELECT YEAR(txn_time) AS txn_year, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM transactions GROUP BY YEAR(txn_time) ORDER BY txn_year DESC;
