SELECT a.id, a.opened_date, a.customer_id FROM accounts a WHERE a.customer_id IN (26, 29, 32) AND a.customer_id NOT IN (SELECT id FROM customers WHERE city = 'Tokyo') ORDER BY opened_date DESC;
