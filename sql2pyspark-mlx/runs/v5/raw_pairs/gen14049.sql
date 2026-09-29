WITH metrics AS (
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        LEAD(balance, 1, 0) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_balance
    FROM accounts
)
SELECT
    id,
    customer_id,
    opened_date,
    status,
    cumulative_balance,
    CASE WHEN status RLIKE '^(OPEN|FROZEN)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS status_zone
FROM metrics
ORDER BY status DESC NULLS FIRST
