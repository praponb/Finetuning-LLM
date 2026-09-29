WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        SUM(balance) OVER (PARTITION BY account_type, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        LEAD(balance, 1) OVER (PARTITION BY customer_id ORDER BY balance, id) AS next_balance
    FROM accounts
)
SELECT
    id,
    customer_id,
    balance,
    opened_date,
    status,
    next_balance
FROM scored;
