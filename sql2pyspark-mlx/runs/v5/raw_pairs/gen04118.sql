SELECT c.city, c.name, s.customer_id, s.country, s.started_at, LENGTH(c.country) AS country_length FROM customers c LEFT OUTER JOIN sessions s ON c.id = s.customer_id
