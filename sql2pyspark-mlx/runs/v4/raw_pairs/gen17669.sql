WITH filtered_accounts AS (
    SELECT *
    FROM accounts
    WHERE customer_id IS NOT NULL AND balance <= 57000 AND opened_date <= '2024-03-01'
),
summary AS (
    SELECT
        customer_id,
        status,
        ROUND(STDDEV(balance), 2) stddev_balance,
        ROUND((100 * SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_open
    FROM filtered_accounts
    GROUP BY customer_id, status
)
SELECT
    customer_id,
    status,
    stddev_balance,
    pct_open
FROM summary
ORDER BY pct_open;
