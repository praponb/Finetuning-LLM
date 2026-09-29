SELECT a.id, a.customer_id, a.status, a.opened_date, c.name, c.email, TRIM(c.country) AS clean_country FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id
