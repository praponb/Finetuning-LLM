SELECT a.id, a.status, a.account_type FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE city IN ('Bangkok', 'Berlin')) ORDER BY 2, 3, id LIMIT 50
