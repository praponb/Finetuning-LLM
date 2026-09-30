SELECT p.country, e.status, COUNT(*) AS rows_total, MAX(e.discount) AS max_discount FROM orders e INNER JOIN customers p ON e.customer_id = p.id GROUP BY p.country, e.status
