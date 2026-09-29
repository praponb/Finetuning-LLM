SELECT customer_id, COUNT(*) AS row_count FROM accounts WHERE MONTH(opened_date) IN (2, 3, 6) GROUP BY customer_id ORDER BY row_count DESC;
