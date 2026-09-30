WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY customer_id, account_type ORDER BY balance NULLS LAST) AS balance_percent_rank,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        AVG(balance) OVER (PARTITION BY status) AS status_avg_balance,
        LEAD(balance, 2) OVER (PARTITION BY account_type ORDER BY balance, id) AS next_balance
    FROM accounts
)
SELECT
    id,
    customer_id,
    balance,
    opened_date,
    balance_percent_rank,
    cumulative_balance,
    status_avg_balance,
    next_balance
FROM with_windows
ORDER BY id NULLS LAST;
