WITH customer_totals AS (
    SELECT customer_id, MAX(opened_date) latest_opened_date, MAX(balance) - MIN(balance) balance_range
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.id,
    c.email,
    c.age,
    ct.latest_opened_date,
    ct.balance_range
FROM customers c
LEFT JOIN customer_totals ct ON c.id = ct.customer_id
WHERE ct.balance_range >= 56
