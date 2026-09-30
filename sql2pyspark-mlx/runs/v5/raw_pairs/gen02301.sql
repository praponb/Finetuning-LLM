SELECT id, status, amount, discount, INITCAP(TRIM(status)) AS status_title FROM orders ORDER BY amount DESC, id LIMIT 3
