SELECT c.id, c.segment
FROM customers c
WHERE c.name LIKE 'G%' AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.account_type != 'SAVINGS')
ORDER BY id DESC;
