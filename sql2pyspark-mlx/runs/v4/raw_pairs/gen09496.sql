SELECT a.id, a.opened_date
FROM accounts a
WHERE a.opened_date BETWEEN '2023-08-01' AND '2024-08-09' AND a.customer_id IN (SELECT id FROM customers WHERE age BETWEEN 43 AND 67);
