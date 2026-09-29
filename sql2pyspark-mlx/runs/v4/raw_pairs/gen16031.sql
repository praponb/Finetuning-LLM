SELECT id, customer_id, opened_date, balance FROM (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS rn FROM accounts) AS r WHERE rn <= 3 ORDER BY customer_id ASC, id DESC
