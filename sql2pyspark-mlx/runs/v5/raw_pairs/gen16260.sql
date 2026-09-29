SELECT
    a.id,
    a.status,
    a.opened_date,
    a.balance,
    c.name,
    c.country,
    c.segment
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.signup_date BETWEEN '2024-07-02' AND '2025-02-01'
ORDER BY segment
