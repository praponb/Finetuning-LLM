SELECT account_id, COUNT(*) AS row_count, SUM(amount) AS total_amount FROM transactions GROUP BY account_id HAVING SUM(amount) < 11000 ORDER BY total_amount, account_id DESC LIMIT 20
