SELECT
    id,
    account_type,
    balance,
    opened_date,
    AVG(balance) OVER w_ordered running_avg_balance,
    COUNT(balance) OVER w_ordered running_count_balance,
    SUM(balance) OVER w_ordered running_sum_balance,
    NTILE(5) OVER (ORDER BY balance DESC, id) balance_bucket
FROM accounts
WINDOW w_ordered AS (PARTITION BY customer_id, account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
