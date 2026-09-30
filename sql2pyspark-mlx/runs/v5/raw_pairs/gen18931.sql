WITH AccountsByCustomer AS (
    SELECT customer_id, SUM(CASE WHEN account_type = 'SAVINGS' THEN balance ELSE 0 END) AS savings_balance, ROUND((100 * SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_closed
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.id,
    c.credit_limit,
    c.signup_date,
    A.savings_balance,
    A.pct_closed
FROM customers c
INNER JOIN AccountsByCustomer A ON c.id = A.customer_id
ORDER BY c.id DESC
