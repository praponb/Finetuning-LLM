WITH CustomerSummary AS (
    SELECT customer_id, MIN(balance) min_balance, MAX(balance) - MIN(balance) balance_range
    FROM accounts
    GROUP BY customer_id
)
SELECT c.age, C.min_balance, C.balance_range
FROM customers c
JOIN CustomerSummary C ON c.id = C.customer_id
WHERE C.min_balance >= 68000
ORDER BY balance_range DESC, min_balance;
