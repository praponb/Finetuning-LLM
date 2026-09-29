SELECT account_id, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM transactions GROUP BY account_id ORDER BY total_amount DESC, rnk DESC;
