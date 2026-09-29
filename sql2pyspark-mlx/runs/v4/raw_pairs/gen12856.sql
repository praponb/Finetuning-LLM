SELECT a.id, a.opened_date, a.balance, a.status FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE segment IN ('SMB', 'Consumer', 'Corporate')) ORDER BY a.id DESC;
