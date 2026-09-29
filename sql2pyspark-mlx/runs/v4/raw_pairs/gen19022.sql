SELECT c.id, c.age, t.max_balance
FROM customers c
JOIN (SELECT customer_id, MAX(balance) AS max_balance
    FROM accounts
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.max_balance > 62000
ORDER BY c.id
LIMIT 10;
