SELECT
    id,
    account_type,
    opened_date,
    status,
    CUME_DIST() OVER (PARTITION BY customer_id ORDER BY balance NULLS LAST) AS balance_cume_dist,
    SUM(balance) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
    PERCENT_RANK() OVER (ORDER BY balance DESC NULLS FIRST) AS balance_percent_rank,
    AVG(balance) OVER w_grp AS balance_avg_3
FROM accounts
WHERE status = 'FROZEN'
WINDOW w_grp AS (PARTITION BY status, customer_id ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
