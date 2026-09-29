WITH CustomerTotals AS (
    SELECT customer_id, MIN(balance) min_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT c.age, C.min_balance
FROM customers c
JOIN CustomerTotals C ON c.id = C.customer_id
ORDER BY c.age ASC
