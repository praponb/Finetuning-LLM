SELECT c.id, c.credit_limit FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.status IN ('FROZEN', 'OPEN')) ORDER BY id DESC;
