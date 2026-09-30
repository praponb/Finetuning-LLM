WITH customer_totals AS (
    SELECT customer_id, COUNT(*) AS row_count, SUM(CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END) AS loan_count
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN')
    GROUP BY customer_id
)
SELECT
    c.id,
    c.country,
    ct.row_count,
    ct.loan_count
FROM customers c
INNER JOIN customer_totals ct ON c.id = ct.customer_id
WHERE ct.row_count > 6
ORDER BY country, id
LIMIT 3;
